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
  const dayOfWeek = date.getDay();
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
  
  // Store time so it displays as the same hour:minute
  // If routines says 8:00 AM, store it so it displays as 8:00 AM
  // We'll store the time directly without timezone offset
  // Device will interpret this as the same hour:minute
  
  const hourStr = String(hour).padStart(2, "0");
  const minuteStr = String(minute).padStart(2, "0");
  
  // Create date in UTC with the exact hour:minute from routines
  // This ensures the time stored matches the time in routines
  // When device displays, it will show the same hour:minute
  const utcDate = new Date(`${year}-${month}-${day}T${hourStr}:${minuteStr}:00Z`);
  
  return admin.firestore.Timestamp.fromDate(utcDate);
}

function shouldGenerateForDay(dayOfWeek, daysOfWeek) {
  if (!daysOfWeek || !Array.isArray(daysOfWeek)) {
    return false;
  }
  return daysOfWeek.includes(dayOfWeek);
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
      daysOfWeek: routine.daysOfWeek,
      targetDayOfWeek: dayOfWeek,
    });
    
    if (!shouldGenerateForDay(dayOfWeek, routine.daysOfWeek)) {
      logger.debug(`Skipping routine ${routineId} - not scheduled for day ${dayOfWeek}`, {
        userId,
        dateKey,
      });
      continue;
    }
    
    if (
      routine.hour === undefined ||
      routine.minute === undefined ||
      !routine.kidID
    ) {
      logger.warn(`Skipping routine ${routineId} - missing required fields`, {
        userId,
        dateKey,
        hasHour: routine.hour !== undefined,
        hasMinute: routine.minute !== undefined,
        hasKidID: !!routine.kidID,
      });
      continue;
    }
    
    // Convert hour from 12-hour format (with AM/PM) to 24-hour format
    let hour24 = routine.hour;
    
    // Check if routine has hourType field (AM/PM) or isPM field
    if (routine.hourType) {
      // hourType: "AM" or "PM"
      if (routine.hourType === "PM" && routine.hour !== 12) {
        hour24 = routine.hour + 12;
      } else if (routine.hourType === "AM" && routine.hour === 12) {
        hour24 = 0; // 12 AM = 00:00
      }
    } else if (routine.isPM !== undefined) {
      // isPM: true or false
      if (routine.isPM && routine.hour !== 12) {
        hour24 = routine.hour + 12;
      } else if (!routine.isPM && routine.hour === 12) {
        hour24 = 0; // 12 AM = 00:00
      }
    }
    // If no AM/PM field, assume hour is already in 24-hour format
    
    const docId = `${routineId}_${dateKey}`;
    if (existingDocIds.has(docId)) {
      logger.debug(`Event already exists: ${docId}`, {
        userId,
        dateKey,
      });
      continue;
    }
    
    const scheduledDateTime = createScheduledDateTime(
      dateInfo,
      hour24,
      routine.minute
    );
    
    logger.info(`Created scheduledDateTime for routine ${routineId}`, {
      userId,
      routineId,
      dateKey,
      hour12: routine.hour,
      hourType: routine.hourType || (routine.isPM !== undefined ? (routine.isPM ? "PM" : "AM") : "24h"),
      hour24: hour24,
      minute: routine.minute,
      scheduledDateTimeUTC: scheduledDateTime.toDate().toISOString(),
    });
    
    const routineEvent = {
      routineId,
      kidID: routine.kidID,
      scheduledDateTime,
      dateKey,
      status: "pending",
      createdBy: "system",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    
    eventsToCreate.push({
      docId,
      event: routineEvent,
    });
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
    const userId = request.auth?.uid;
    
    if (!userId) {
      throw new Error("User must be authenticated");
    }
    
    logger.info("Manual trigger: generateTodayRoutineEvents", {
      timestamp: new Date().toISOString(),
      userId,
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
