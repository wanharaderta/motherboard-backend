const admin = require("firebase-admin");
const {onSchedule} = require("firebase-functions/v2/scheduler");
const {onCall} = require("firebase-functions/v2/https");
const logger = require("firebase-functions/logger");

admin.initializeApp();

const db = admin.firestore();

function getDateInfo(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  // iOS uses 1..7 (Mon=1..Sat=6, Sun=7). JS getDay(): Sun=0..Sat=6
  const jsDayOfWeek = date.getDay();
  const dayOfWeek = jsDayOfWeek === 0 ? 7 : jsDayOfWeek;
  const dateKey = `${year}-${month}-${day}`;
  
  return {
    year,
    month,
    day,
    dayOfWeek,
    dateKey,
  };
}

function getTodayInTimezone() {
  const now = new Date();
  return getDateInfo(now);
}

function getTomorrowInTimezone() {
  const now = new Date();
  const tomorrow = new Date(now);
  tomorrow.setDate(tomorrow.getDate() + 1);
  return getDateInfo(tomorrow);
}

function createScheduledDateTime(dateInfo, hour, minute) {
  const {year, month, day} = dateInfo;
  
  // Device dan server menggunakan timezone US yang sama (America/New_York)
  // Jadi langsung set hour:minute sesuai routine tanpa konversi
  // Contoh: routine 8:00 AM -> scheduledDateTime juga 8:00 AM (ketika di-display di America/New_York)
  
  const hourStr = String(hour).padStart(2, "0");
  const minuteStr = String(minute).padStart(2, "0");
  
  // Kita perlu membuat UTC timestamp yang mewakili waktu tersebut di America/New_York
  // Cara: buat date di UTC yang ketika di-convert ke America/New_York akan menunjukkan hour:minute yang diinginkan
  
  // Dapatkan offset timezone America/New_York untuk tanggal tersebut
  // dengan membuat date di UTC dan melihat perbedaannya dengan waktu di America/New_York
  const testUTC = new Date(`${year}-${month}-${day}T12:00:00Z`);
  
  // Format test date sebagai string dalam timezone America/New_York
  const usTimeString = testUTC.toLocaleString("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  
  // Parse US time string (format: "MM/DD/YYYY, HH:mm:ss")
  const [datePart, timePart] = usTimeString.split(", ");
  const [usMonth, usDay, usYear] = datePart.split("/");
  const [usHour, usMinute, usSecond] = timePart.split(":");
  
  // Hitung offset: jika 12:00 UTC = 7:00 AM EST (UTC-5), maka offset = 5 jam
  // Jika 12:00 UTC = 8:00 AM EDT (UTC-4), maka offset = 4 jam
  const usHourInt = parseInt(usHour);
  const offsetHours = 12 - usHourInt; // Perbedaan jam antara UTC dan US time (dalam jam)
  
  // Buat UTC timestamp untuk waktu yang diinginkan di America/New_York
  // Jika kita ingin 8:00 AM di America/New_York, dan offset adalah 5 jam (EST),
  // maka UTC time = 8:00 + 5 = 13:00 UTC
  // Cara: buat date di UTC dengan waktu yang sudah di-adjust dengan offset
  const baseUTC = new Date(`${year}-${month}-${day}T00:00:00Z`);
  const targetTimeInMs = (hour * 60 * 60 * 1000) + (minute * 60 * 1000);
  const offsetInMs = offsetHours * 60 * 60 * 1000;
  const finalDate = new Date(baseUTC.getTime() + targetTimeInMs + offsetInMs);
  
  return admin.firestore.Timestamp.fromDate(finalDate);
}

function createScheduledDateTimeFromStartDate(startDate, targetDateInfo) {
  // Event dimulai dari original startDate
  // Untuk hari berikutnya, hanya ubah tanggalnya saja (jam dan menit tetap sama)
  
  if (!startDate) {
    throw new Error("startDate is required");
  }
  
  // Convert Firestore Timestamp ke Date
  const startDateObj = startDate.toDate();
  
  // Ambil tanggal dari startDate untuk dibandingkan dengan targetDateInfo
  const startYear = startDateObj.getUTCFullYear();
  const startMonth = String(startDateObj.getUTCMonth() + 1).padStart(2, "0");
  const startDay = String(startDateObj.getUTCDate()).padStart(2, "0");
  const startDateKey = `${startYear}-${startMonth}-${startDay}`;
  
  // Jika targetDate sama dengan tanggal startDate, gunakan startDate asli
  const {year, month, day} = targetDateInfo;
  const targetDateKey = `${year}-${month}-${day}`;
  
  if (startDateKey === targetDateKey) {
    // Gunakan startDate asli untuk event pertama
    return startDate;
  }
  
  // Untuk hari berikutnya: ambil jam dan menit dari startDate, ubah tanggal ke targetDate
  const hours = startDateObj.getUTCHours();
  const minutes = startDateObj.getUTCMinutes();
  const seconds = startDateObj.getUTCSeconds();
  
  const hourStr = String(hours).padStart(2, "0");
  const minuteStr = String(minutes).padStart(2, "0");
  const secondStr = String(seconds).padStart(2, "0");
  
  // Buat UTC date dengan tanggal baru tapi waktu yang sama dari startDate
  const newDate = new Date(`${year}-${month}-${day}T${hourStr}:${minuteStr}:${secondStr}Z`);
  
  return admin.firestore.Timestamp.fromDate(newDate);
}

function shouldGenerateForDay(dayOfWeek, daysOfWeek) {
  if (!daysOfWeek || !Array.isArray(daysOfWeek)) {
    return false;
  }
  return daysOfWeek.includes(dayOfWeek);
}

function shouldGenerateForInterval(startDate, targetDateInfo, intervalValue, intervalUnit) {
  // Check apakah targetDate sesuai dengan interval pattern dari startDate
  if (!startDate || !intervalValue || !intervalUnit) {
    return false;
  }
  
  const startDateObj = startDate.toDate();
  const targetDateObj = new Date(`${targetDateInfo.year}-${targetDateInfo.month}-${targetDateInfo.day}T00:00:00Z`);
  
  // Hitung selisih waktu dari startDate ke targetDate
  const diffMs = targetDateObj.getTime() - startDateObj.getTime();
  const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  
  if (intervalUnit === "day" || intervalUnit === "days") {
    // Check apakah diffDays adalah kelipatan dari intervalValue
    return diffDays >= 0 && diffDays % intervalValue === 0;
  } else if (intervalUnit === "week" || intervalUnit === "weeks") {
    // Check apakah diffDays adalah kelipatan dari (intervalValue * 7)
    const diffWeeks = Math.floor(diffDays / 7);
    return diffDays >= 0 && diffWeeks % intervalValue === 0;
  } else if (intervalUnit === "hour" || intervalUnit === "hours") {
    // Untuk interval per jam, generate untuk semua hari (akan di-handle di level event creation)
    // Tapi untuk sekarang, kita generate per hari, jadi return true jika hari sama atau setelah startDate
    return diffDays >= 0;
  } else if (intervalUnit === "minute" || intervalUnit === "minutes") {
    // Untuk interval per menit, generate untuk semua hari
    return diffDays >= 0;
  }
  
  return false;
}

function getIntervalMs(intervalValue, intervalUnit) {
  if (!intervalValue || !intervalUnit) return null;
  const unit = String(intervalUnit).toLowerCase();
  if (unit === "minute" || unit === "minutes") return intervalValue * 60 * 1000;
  if (unit === "hour" || unit === "hours") return intervalValue * 60 * 60 * 1000;
  if (unit === "day" || unit === "days") return intervalValue * 24 * 60 * 60 * 1000;
  if (unit === "week" || unit === "weeks") return intervalValue * 7 * 24 * 60 * 60 * 1000;
  return null;
}

function getEffectiveStartDate(routine) {
  // Prefer startDate; fallback to createdAt/updatedAt if needed
  return routine.startDate || routine.createdAt || routine.updatedAt || null;
}

function getDateKeyFromTimestamp(ts) {
  if (!ts) return null;
  const d = ts.toDate();
  return getDateInfo(d).dateKey;
}

function getDateWindowUtcMs(dateInfo) {
  const startMs = Date.parse(`${dateInfo.year}-${dateInfo.month}-${dateInfo.day}T00:00:00Z`);
  const endMs = startMs + 24 * 60 * 60 * 1000;
  return {startMs, endMs};
}

function generateScheduledDateTimesForDate(routine, routineId, dateInfo) {
  // Returns array of Firestore Timestamps that fall within the target date window
  const scheduled = [];

  const startTs = getEffectiveStartDate(routine);
  if (!startTs) return scheduled;

  const startMs = startTs.toDate().getTime();
  const endMs = routine.endDate ? routine.endDate.toDate().getTime() : null;
  if (endMs !== null && endMs < startMs) {
    // Bad data: endDate before startDate
    return scheduled;
  }

  const {startMs: dayStartMs, endMs: dayEndMs} = getDateWindowUtcMs(dateInfo);

  // Filter by repetitionType first (date-level filtering)
  const repetitionType = routine.repetitionType;
  const dayOfWeek = dateInfo.dayOfWeek;

  if (repetitionType === "oneTime") {
    const startDateKey = getDateKeyFromTimestamp(startTs);
    if (dateInfo.dateKey !== startDateKey) return scheduled;
  } else if (repetitionType === "weekly") {
    // If daysOfWeek missing, assume every day (1..7)
    const days = Array.isArray(routine.daysOfWeek) && routine.daysOfWeek.length > 0
      ? routine.daysOfWeek
      : [1, 2, 3, 4, 5, 6, 7];
    if (!shouldGenerateForDay(dayOfWeek, days)) return scheduled;
  } else if (repetitionType === "interval") {
    // handled below via intervalValue/unit if present
  } else if (repetitionType === "daily") {
    // always eligible (after startDate), time window checks below
  } else if (!repetitionType) {
    // backward compatibility: eligible after startDate; endDate checked below
  }

  // Interval handling
  const intervalMs = getIntervalMs(routine.intervalValue, routine.intervalUnit);
  const unit = routine.intervalUnit ? String(routine.intervalUnit).toLowerCase() : null;
  const hasHourMinuteInterval = intervalMs !== null && (unit === "hour" || unit === "hours" || unit === "minute" || unit === "minutes");
  const hasDayWeekInterval = intervalMs !== null && (unit === "day" || unit === "days" || unit === "week" || unit === "weeks");

  // If interval is day/week, do a date-level check (based on startDate)
  if (intervalMs !== null && hasDayWeekInterval) {
    if (!shouldGenerateForInterval(startTs, dateInfo, routine.intervalValue, routine.intervalUnit)) {
      return scheduled;
    }
  }

  // Generate one or many timestamps within the day
  if (intervalMs !== null && hasHourMinuteInterval) {
    // Generate occurrences aligned to startDate every intervalMs, within this day
    const firstCandidateMs = Math.max(dayStartMs, startMs);
    const n = Math.ceil((firstCandidateMs - startMs) / intervalMs);
    let t = startMs + n * intervalMs;
    while (t < dayEndMs) {
      if (t >= startMs && (endMs === null || t <= endMs)) {
        scheduled.push(admin.firestore.Timestamp.fromMillis(t));
      }
      t += intervalMs;
    }
    return scheduled;
  }

  // Non-hour/minute interval: exactly one event per eligible day
  const scheduledTs = routine.startDate
    ? createScheduledDateTimeFromStartDate(startTs, dateInfo)
    : null;

  if (!scheduledTs) return scheduled;

  const t = scheduledTs.toDate().getTime();
  if (t < startMs) return scheduled;
  if (endMs !== null && t > endMs) return scheduled;
  if (t < dayStartMs || t >= dayEndMs) return scheduled;

  scheduled.push(scheduledTs);
  return scheduled;
}

async function ensureEventsForDate(dateInfo, routinesSnapshot, userId, deleteExisting = false) {
  const {dateKey, dayOfWeek} = dateInfo;
  
  logger.info(`Processing date: ${dateKey} (day of week: ${dayOfWeek}) for user: ${userId}`, {
    deleteExisting,
  });
  
  const existingEventsSnapshot = await db
    .collection("users")
    .doc(userId)
    .collection("routineEvents")
    .where("dateKey", "==", dateKey)
    .get();
  
  let existingDocIds;
  
  // If deleteExisting is true, delete all existing events for this date
  if (deleteExisting && !existingEventsSnapshot.empty) {
    logger.info(`Deleting ${existingEventsSnapshot.size} existing events for ${dateKey}`, {
      userId,
      dateKey,
    });
    
    const batchSize = 500;
    const existingDocs = existingEventsSnapshot.docs;
    
    for (let i = 0; i < existingDocs.length; i += batchSize) {
      const batch = db.batch();
      const batchItems = existingDocs.slice(i, i + batchSize);
      
      for (const doc of batchItems) {
        batch.delete(doc.ref);
      }
      
      await batch.commit();
      logger.info(`Deleted batch: ${Math.min(i + batchSize, existingDocs.length)}/${existingDocs.length} events`, {
        userId,
        dateKey,
      });
    }
    
    logger.info(`Successfully deleted all existing events for ${dateKey}`, {
      userId,
      dateKey,
      deletedCount: existingDocs.length,
    });
    
    // Clear existingDocIds since we deleted all events
    existingDocIds = new Set();
  } else {
    // Only create Set if we didn't delete
    existingDocIds = new Set(
      existingEventsSnapshot.docs.map((doc) => doc.id)
    );
  }
  
  const eventsToCreate = [];
  
  for (const routineDoc of routinesSnapshot.docs) {
    const routine = routineDoc.data();
    const routineId = routineDoc.id;
    
    logger.debug(`Processing routine ${routineId}`, {
      userId,
      dateKey,
      routineId,
      repetitionType: routine.repetitionType,
      daysOfWeek: routine.daysOfWeek,
      targetDayOfWeek: dayOfWeek,
    });
    
    // Check required fields (new structure: startDate; old structure: hour/minute)
    const effectiveStartDate = getEffectiveStartDate(routine);
    if (!effectiveStartDate && (routine.hour === undefined || routine.minute === undefined)) {
      logger.warn(`Skipping routine ${routineId} - missing required fields (startDate or hour/minute)`, {
        userId,
        dateKey,
        hasStartDate: !!routine.startDate,
        hasHour: routine.hour !== undefined,
        hasMinute: routine.minute !== undefined,
        hasKidID: !!routine.kidID,
      });
      continue;
    }
    
    // Generate 0..N scheduledDateTimes for this routine on this date
    const scheduledDateTimes = generateScheduledDateTimesForDate(routine, routineId, dateInfo);
    if (scheduledDateTimes.length === 0) {
      logger.debug(`No scheduledDateTimes for routine ${routineId} on ${dateKey}`, {
        userId,
        routineId,
        dateKey,
      });
      continue;
    }
    
    if (!routine.kidID) {
      logger.warn(`Skipping routine ${routineId} - missing kidID`, {
        userId,
        dateKey,
      });
      continue;
    }

    for (const scheduledDateTime of scheduledDateTimes) {
      const docId = `${routineId}_${scheduledDateTime.toMillis()}`;
      if (existingDocIds.has(docId)) {
        continue;
      }

      const routineEvent = {
        routineId,
        kidID: routine.kidID,
        scheduledDateTime,
        dateKey,
        activityName: routine.activityName,
        code: routine.code,
        title: routine.title,
        status: "pending",
        createdBy: "system",
        createdAt: admin.firestore.FieldValue.serverTimestamp(),
        // Notification fields
        notificationSent: false,
        notificationSentAt: null,
      };

      eventsToCreate.push({
        docId,
        event: routineEvent,
      });
    }
  }
  
  if (eventsToCreate.length === 0) {
    logger.info(`No new events to create for ${dateKey}`);
    return 0;
  }
  
  logger.info(`Creating ${eventsToCreate.length} events for ${dateKey}`, {
    userId,
    dateKey,
    eventsCount: eventsToCreate.length,
  });
  
  const batchSize = 500;
  let createdCount = 0;
  
  for (let i = 0; i < eventsToCreate.length; i += batchSize) {
    const batch = db.batch();
    const batchItems = eventsToCreate.slice(i, i + batchSize);
    
    logger.info(`Preparing batch ${Math.floor(i / batchSize) + 1} with ${batchItems.length} items`, {
      userId,
      dateKey,
    });
    
    for (const {docId, event} of batchItems) {
      const eventRef = db
        .collection("users")
        .doc(userId)
        .collection("routineEvents")
        .doc(docId);
      batch.set(eventRef, event);
      logger.debug(`Adding event to batch: ${docId}`, {
        userId,
        dateKey,
        routineId: event.routineId,
      });
    }
    
    try {
      await batch.commit();
      createdCount += batchItems.length;
      logger.info(`Batch committed successfully: ${createdCount}/${eventsToCreate.length} events`, {
        userId,
        dateKey,
      });
    } catch (batchError) {
      logger.error("Error committing batch", {
        userId,
        dateKey,
        error: batchError.message,
        stack: batchError.stack,
      });
      throw batchError;
    }
  }
  
  logger.info(`Successfully created ${createdCount} events for ${dateKey}`, {
    userId,
    dateKey,
    totalCreated: createdCount,
  });
  return createdCount;
}

async function ensureRoutineEventsLogic(userId) {
  logger.info("Starting ensureRoutineEvents", {
    timestamp: new Date().toISOString(),
    userId,
  });
  
  if (!userId) {
    throw new Error("UserId is required");
  }
  
  // Query routines from user's subcollection
  const routinesSnapshot = await db
    .collection("users")
    .doc(userId)
    .collection("routines")
    .where("isActive", "==", true)
    .get();
  
  logger.info(`Querying routines for user ${userId}`, {
    routinesFound: routinesSnapshot.size,
    empty: routinesSnapshot.empty,
  });
  
  if (routinesSnapshot.empty) {
    logger.info("No active routines found", {userId});
    return {
      todayEventsCreated: 0,
      tomorrowEventsCreated: 0,
      totalRoutines: 0,
    };
  }
  
  logger.info(`Found ${routinesSnapshot.size} active routines for user: ${userId}`);
  
  const todayInfo = getTodayInTimezone();
  const tomorrowInfo = getTomorrowInTimezone();
  
  logger.info("Ensuring events for today and tomorrow", {
    userId,
    today: todayInfo.dateKey,
    tomorrow: tomorrowInfo.dateKey,
  });
  
  // Idempotent: only create if not exists (no delete)
  const todayCount = await ensureEventsForDate(todayInfo, routinesSnapshot, userId, false);
  const tomorrowCount = await ensureEventsForDate(tomorrowInfo, routinesSnapshot, userId, false);
  
  const result = {
    todayEventsCreated: todayCount,
    tomorrowEventsCreated: tomorrowCount,
    totalRoutines: routinesSnapshot.size,
    today: todayInfo.dateKey,
    tomorrow: tomorrowInfo.dateKey,
  };
  
  logger.info("ensureRoutineEvents completed", {...result, userId});
  return result;
}

exports.ensureRoutineEvents = onSchedule(
  {
    schedule: "every day 00:05",
    timeZone: "America/New_York",
    memory: "256MiB",
    maxInstances: 1,
  },
  async (event) => {
    try {
      // Get all users and generate events for each
      const usersSnapshot = await db.collection("users").get();
      
      if (usersSnapshot.empty) {
        logger.info("No users found");
        return;
      }
      
      logger.info(`Found ${usersSnapshot.size} users, generating events for each`);
      
      for (const userDoc of usersSnapshot.docs) {
        const userId = userDoc.id;
        try {
          await ensureRoutineEventsLogic(userId);
        } catch (error) {
          logger.error(`Error generating events for user ${userId}`, {
            error: error.message,
            stack: error.stack,
          });
          // Continue with other users even if one fails
        }
      }
    } catch (error) {
      logger.error("Error in ensureRoutineEvents", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }
);

exports.generateTodayRoutineEvents = onCall(
  {
    memory: "256MiB",
    maxInstances: 1,
  },
  async (request) => {
    // Bisa menerima userId dari request.data (untuk admin/testing)
    // atau menggunakan request.auth?.uid (untuk user yang sedang login)
    const userId = request.data?.userId || request.auth?.uid;
    
    if (!userId) {
      throw new Error("User must be authenticated or userId must be provided in request data");
    }
    
    logger.info("Manual trigger: generateTodayRoutineEvents", {
      timestamp: new Date().toISOString(),
      userId,
      providedViaAuth: !!request.auth?.uid,
      providedViaData: !!request.data?.userId,
    });
    
    try {
      // Query routines from user's subcollection
      const routinesSnapshot = await db
        .collection("users")
        .doc(userId)
        .collection("routines")
        .where("isActive", "==", true)
        .get();
      
      if (routinesSnapshot.empty) {
        logger.info("No active routines found", {userId});
        return {
          success: true,
          todayEventsCreated: 0,
          tomorrowEventsCreated: 0,
          totalRoutines: 0,
          message: "No active routines found",
        };
      }
      
      logger.info(`Found ${routinesSnapshot.size} active routines for user: ${userId}`);
      
      const todayInfo = getTodayInTimezone();
      const tomorrowInfo = getTomorrowInTimezone();
      
      logger.info("Generating events for today and tomorrow (delete and recreate)", {
        userId,
        today: todayInfo.dateKey,
        tomorrow: tomorrowInfo.dateKey,
      });
      
      // Delete and recreate events for both today and tomorrow
      const todayCount = await ensureEventsForDate(todayInfo, routinesSnapshot, userId, true);
      const tomorrowCount = await ensureEventsForDate(tomorrowInfo, routinesSnapshot, userId, true);
      
      const result = {
        todayEventsCreated: todayCount,
        tomorrowEventsCreated: tomorrowCount,
        totalRoutines: routinesSnapshot.size,
        today: todayInfo.dateKey,
        tomorrow: tomorrowInfo.dateKey,
      };
      
      logger.info("generateTodayRoutineEvents completed", {
        userId,
        ...result,
      });
      
      return {
        success: true,
        ...result,
        message: "Today's and tomorrow's routine events generated successfully. Events were recreated.",
      };
    } catch (error) {
      logger.error("Error in generateTodayRoutineEvents", {
        userId,
        error: error.message,
        stack: error.stack,
      });
      throw new Error(`Failed to generate routine events: ${error.message}`);
    }
  }
);

// Function to send notification for a routine event
async function sendRoutineEventNotification(userId, eventId, eventData) {
  try {
    const {routineId, kidID, scheduledDateTime, activityName, code, title, dateKey} = eventData;
    
    // Get user's FCM tokens
    const userDoc = await db.collection("users").doc(userId).get();
    if (!userDoc.exists) {
      logger.warn(`User ${userId} not found`);
      return false;
    }
    
    const userData = userDoc.data();
    const fcmTokens = userData.fcmTokens || [];
    
    if (fcmTokens.length === 0) {
      logger.warn(`No FCM tokens found for user ${userId}`);
      return false;
    }
    
    // Prepare notification message
    const notificationTitle = title || activityName || "Routine Reminder";
    const notificationBody = `Time for ${notificationTitle}`;
    
    const message = {
      notification: {
        title: notificationTitle,
        body: notificationBody,
      },
      data: {
        type: "routine_event",
        routineId,
        kidID,
        eventId,
        scheduledDateTime: scheduledDateTime.toDate().toISOString(),
        dateKey,
        code: code || "",
        activityName: activityName || "",
      },
      tokens: fcmTokens,
    };
    
    // Send notification
    const response = await admin.messaging().sendEachForMulticast(message);
    
    logger.info(`Notification sent for routine event ${routineId}_${dateKey}`, {
      userId,
      routineId,
      dateKey,
      successCount: response.successCount,
      failureCount: response.failureCount,
    });
    
    // Update event with notification status
    // Always update status to "completed" regardless of notification success
    const eventRef = db
      .collection("users")
      .doc(userId)
      .collection("routineEvents")
      .doc(eventId);
    const sentOk = response.successCount > 0;

    await eventRef.update({
      status: "completed",
      notificationSent: sentOk,
      notificationSentAt: sentOk ? admin.firestore.FieldValue.serverTimestamp() : null,
    });

    if (!sentOk) {
      logger.warn(`Notification send had zero successes for ${eventId}, but status updated to completed`, {
        userId,
        routineId,
        dateKey,
        failureCount: response.failureCount,
        tokensCount: fcmTokens.length,
      });
    }
    
    // Save notification history to separate collection
    const notificationHistoryRef = db
      .collection("users")
      .doc(userId)
      .collection("notificationHistory")
      .doc();
    
    const notificationRecord = {
      eventId,
      routineId,
      kidID,
      activityName,
      code,
      title,
      scheduledDateTime,
      dateKey,
      sentAt: admin.firestore.FieldValue.serverTimestamp(),
      successCount: response.successCount,
      failureCount: response.failureCount,
      tokensCount: fcmTokens.length,
      notificationTitle,
      notificationBody,
    };
    
    await notificationHistoryRef.set(notificationRecord);
    
    logger.info(`Notification history saved for event ${routineId}_${dateKey}`, {
      userId,
      notificationId: notificationHistoryRef.id,
    });
    
    return sentOk;
  } catch (error) {
    logger.error("Error sending notification", {
      userId,
      error: error.message,
      stack: error.stack,
    });
    return false;
  }
}

// Scheduled function to check and send notifications for upcoming events
exports.checkAndSendRoutineNotifications = onSchedule(
  {
    schedule: "every 5 minutes",
    timeZone: "America/New_York",
    memory: "256MiB",
    maxInstances: 1,
  },
  async (event) => {
    try {
      logger.info("Starting checkAndSendRoutineNotifications");
      
      const now = new Date();
      const fiveMinutesFromNow = new Date(now.getTime() + 5 * 60 * 1000);
      
      // Get all users
      const usersSnapshot = await db.collection("users").get();
      
      if (usersSnapshot.empty) {
        logger.info("No users found");
        return;
      }
      
      logger.info(`Found ${usersSnapshot.size} users, checking events for each`);
      
      for (const userDoc of usersSnapshot.docs) {
        const userId = userDoc.id;
        
        try {
          // Query events that need notification
          // Events that are pending, scheduled within next 5 minutes, and not yet notified
          const eventsSnapshot = await db
            .collection("users")
            .doc(userId)
            .collection("routineEvents")
            .where("status", "==", "pending")
            .where("notificationSent", "==", false)
            .where("scheduledDateTime", ">=", admin.firestore.Timestamp.fromDate(now))
            .where("scheduledDateTime", "<=", admin.firestore.Timestamp.fromDate(fiveMinutesFromNow))
            .get();
          
          if (eventsSnapshot.empty) {
            continue;
          }
          
          logger.info(`Found ${eventsSnapshot.size} events to notify for user ${userId}`);
          
          for (const eventDoc of eventsSnapshot.docs) {
            const eventData = eventDoc.data();
            await sendRoutineEventNotification(userId, eventDoc.id, eventData);
          }
        } catch (error) {
          logger.error(`Error processing notifications for user ${userId}`, {
            error: error.message,
            stack: error.stack,
          });
          // Continue with other users
        }
      }
      
      logger.info("checkAndSendRoutineNotifications completed");
    } catch (error) {
      logger.error("Error in checkAndSendRoutineNotifications", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }
);

// Function to get upcoming routine events
exports.getUpcomingRoutineEvents = onCall(
  {
    memory: "256MiB",
    maxInstances: 10,
  },
  async (request) => {
    const userId = request.data?.userId || request.auth?.uid;
    
    if (!userId) {
      throw new Error("User must be authenticated or userId must be provided");
    }
    
    const limit = request.data?.limit || 10;
    const daysAhead = request.data?.daysAhead || 7; // Default 7 days ahead
    
    try {
      const now = new Date();
      const futureDate = new Date(now.getTime() + daysAhead * 24 * 60 * 60 * 1000);
      
      // Query upcoming events
      const eventsSnapshot = await db
        .collection("users")
        .doc(userId)
        .collection("routineEvents")
        .where("status", "==", "pending")
        .where("scheduledDateTime", ">=", admin.firestore.Timestamp.fromDate(now))
        .where("scheduledDateTime", "<=", admin.firestore.Timestamp.fromDate(futureDate))
        .orderBy("scheduledDateTime", "asc")
        .limit(limit)
        .get();
      
      const events = eventsSnapshot.docs.map((doc) => ({
        id: doc.id,
        ...doc.data(),
        scheduledDateTime: doc.data().scheduledDateTime.toDate().toISOString(),
        createdAt: doc.data().createdAt?.toDate()?.toISOString(),
        notificationSentAt: doc.data().notificationSentAt?.toDate()?.toISOString(),
      }));
      
      return {
        success: true,
        events,
        count: events.length,
      };
    } catch (error) {
      logger.error("Error in getUpcomingRoutineEvents", {
        userId,
        error: error.message,
        stack: error.stack,
      });
      throw new Error(`Failed to get upcoming routine events: ${error.message}`);
    }
  }
);

// Function to get notification history
exports.getNotificationHistory = onCall(
  {
    memory: "256MiB",
    maxInstances: 10,
  },
  async (request) => {
    const userId = request.data?.userId || request.auth?.uid;
    
    if (!userId) {
      throw new Error("User must be authenticated or userId must be provided");
    }
    
    const limit = request.data?.limit || 50;
    const daysBack = request.data?.daysBack || 30; // Default 30 days back
    
    try {
      const now = new Date();
      const pastDate = new Date(now.getTime() - daysBack * 24 * 60 * 60 * 1000);
      
      // Query notification history from separate collection
      const notificationsSnapshot = await db
        .collection("users")
        .doc(userId)
        .collection("notificationHistory")
        .where("sentAt", ">=", admin.firestore.Timestamp.fromDate(pastDate))
        .orderBy("sentAt", "desc")
        .limit(limit)
        .get();
      
      const notifications = notificationsSnapshot.docs.map((doc) => {
        const data = doc.data();
        return {
          id: doc.id,
          eventId: data.eventId,
          routineId: data.routineId,
          kidID: data.kidID,
          activityName: data.activityName,
          code: data.code,
          title: data.title,
          scheduledDateTime: data.scheduledDateTime.toDate().toISOString(),
          dateKey: data.dateKey,
          sentAt: data.sentAt.toDate().toISOString(),
          successCount: data.successCount,
          failureCount: data.failureCount,
          tokensCount: data.tokensCount,
          notificationTitle: data.notificationTitle,
          notificationBody: data.notificationBody,
        };
      });
      
      return {
        success: true,
        notifications,
        count: notifications.length,
      };
    } catch (error) {
      logger.error("Error in getNotificationHistory", {
        userId,
        error: error.message,
        stack: error.stack,
      });
      throw new Error(`Failed to get notification history: ${error.message}`);
    }
  }
);

// Scheduled function to mark past pending routine events as completed
// Runs every 5 minutes and marks events with scheduledDateTime <= now and status == 'pending'
// This prevents events from remaining in 'pending' after their scheduled time has passed.
exports.markPastRoutineEventsCompleted = onSchedule(
  {
    schedule: "every 5 minutes",
    timeZone: "America/New_York",
    memory: "256MiB",
    maxInstances: 1,
  },
  async (event) => {
    try {
      logger.info("Starting markPastRoutineEventsCompleted");

      const now = new Date();
      const nowTs = admin.firestore.Timestamp.fromDate(now);

      const usersSnapshot = await db.collection("users").get();
      if (usersSnapshot.empty) {
        logger.info("No users found");
        return;
      }

      for (const userDoc of usersSnapshot.docs) {
        const userId = userDoc.id;

        try {
          const eventsSnapshot = await db
            .collection("users")
            .doc(userId)
            .collection("routineEvents")
            .where("status", "==", "pending")
            .where("scheduledDateTime", "<=", nowTs)
            .limit(500)
            .get();

          if (eventsSnapshot.empty) {
            continue;
          }

          const batch = db.batch();
          for (const doc of eventsSnapshot.docs) {
            batch.update(doc.ref, {
              status: "completed",
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            });
          }

          await batch.commit();

          logger.info(`Marked ${eventsSnapshot.size} past events as completed for user ${userId}`);
        } catch (err) {
          logger.error(`Error marking past events for user ${userId}`, {
            userId,
            error: err.message,
            stack: err.stack,
          });
          // continue to next user
        }
      }

      logger.info("markPastRoutineEventsCompleted completed");
    } catch (error) {
      logger.error("Error in markPastRoutineEventsCompleted", {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }
);

// Callable admin endpoint to trigger immediate mark-as-completed for past events (useful for testing)
exports.markPastRoutineEventsCompletedNow = onCall(
  {
    memory: "256MiB",
    maxInstances: 1,
  },
  async (request) => {
    const userId = request.data?.userId || request.auth?.uid;

    if (!userId) {
      throw new Error("User must be authenticated or userId must be provided");
    }

    logger.info("Manual trigger: markPastRoutineEventsCompletedNow", {
      timestamp: new Date().toISOString(),
      userId,
      providedViaAuth: !!request.auth?.uid,
      providedViaData: !!request.data?.userId,
    });

    try {
      const now = new Date();
      const nowTs = admin.firestore.Timestamp.fromDate(now);
      let totalUpdated = 0;

      while (true) {
        const eventsSnapshot = await db
          .collection("users")
          .doc(userId)
          .collection("routineEvents")
          .where("status", "==", "pending")
          .where("scheduledDateTime", "<=", nowTs)
          .limit(500)
          .get();

        if (eventsSnapshot.empty) break;

        const batch = db.batch();
        for (const doc of eventsSnapshot.docs) {
          batch.update(doc.ref, {
            status: "completed",
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        }

        await batch.commit();
        totalUpdated += eventsSnapshot.size;

        if (eventsSnapshot.size < 500) break;
      }

      logger.info(`Marked ${totalUpdated} past events as completed for user ${userId}`);

      return {
        success: true,
        updatedCount: totalUpdated,
        userId,
        timestamp: now.toISOString(),
      };
    } catch (error) {
      logger.error("Error in markPastRoutineEventsCompletedNow", {
        userId,
        error: error.message,
        stack: error.stack,
      });
      throw new Error(`Failed to mark past events: ${error.message}`);
    }
  }
);
