// server.js - גרסה סופית עם getDifference מיידי

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const Parser = require('rss-parser');
const cheerio = require('cheerio');

const { Api, TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { Raw } = require('telegram/events');

const app = express();
app.use(cors());

const parser = new Parser({
    timeout: 8000,
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' }
});

let newsList = [];
let clients = [];
const MAX_NEWS = 1000;

const apiId = parseInt(process.env.TELEGRAM_API_ID);
const apiHash = process.env.TELEGRAM_API_HASH;
const sessionString = process.env.TELEGRAM_SESSION || process.env.SESSION_STRING || "";

const rssChannels = [
    { name: "JDN (אתר)", url: "https://www.jdn.co.il/feed/" },
    { name: "ערוץ 7 (אתר)", url: "https://www.inn.co.il/Rss.aspx?Category=1" },
    { name: "סרוגים (אתר)", url: "https://www.srugim.co.il/feed" },
    { name: "המחדש (אתר)", url: "https://hm-news.co.il/feed/" },
    { name: "בחדרי חרדים (אתר)", url: "https://www.bhol.co.il/rss.xml" },
    { name: "ערוץ 14 (אתר)", url: "https://www.now14.co.il/feed/" }
];

// ==========================================
// לוגים
// ==========================================

const LATENCY_LOG = [];
const MAX_LATENCY_LOG = 200;

function logLatency(source, stage, telegramTimestamp, extraInfo = '') {
    const now = Date.now();
    const telegramMs = telegramTimestamp * 1000;
    const serverDelayMs = now - telegramMs;
    const delay = serverDelayMs / 1000;

    const entry = {
        time: new Date(now).toISOString(),
        source, stage,
        telegramTime: new Date(telegramMs).toISOString(),
        serverDelaySeconds: delay.toFixed(1),
        extraInfo
    };

    LATENCY_LOG.unshift(entry);
    if (LATENCY_LOG.length > MAX_LATENCY_LOG) LATENCY_LOG.pop();

    const icon = delay < 3 ? '🟢' : delay < 15 ? '🟡' : '🔴';
    console.log(`${icon} [LATENCY] ${stage} | ${source} | עיכוב: ${delay.toFixed(1)}s | ${extraInfo}`);
    return entry;
}

app.get('/latency', (req, res) => {
    res.json({
        totalLogged: LATENCY_LOG.length,
        summary: {
            avgServerDelay: LATENCY_LOG.length
                ? (LATENCY_LOG.reduce((s, e) => s + parseFloat(e.serverDelaySeconds), 0) / LATENCY_LOG.length).toFixed(1) + 's'
                : 'N/A',
            connectedClients: clients.length
        },
        log: LATENCY_LOG
    });
});

// ==========================================
// SSE ו-API
// ==========================================

app.get('/', (req, res) => res.json(newsList));
app.get('/ping', (req, res) => res.send('pong'));

app.get('/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    const clientId = Date.now();
    clients.push({ id: clientId, res });
    console.log(`📡 לקוח התחבר (סה"כ: ${clients.length})`);
    res.write(`data: ${JSON.stringify({ type: 'connected', time: new Date().toISOString() })}\n\n`);

    req.on('close', () => {
        clients = clients.filter(c => c.id !== clientId);
        console.log(`📡 לקוח התנתק (נותרו: ${clients.length})`);
    });
});

setInterval(() => clients.forEach(c => c.res.write(':\n\n')), 25000);

function generateHash(text) {
    return crypto.createHash('md5').update(text).digest('hex');
}

function broadcast(newsItem, telegramDate) {
    if (telegramDate && clients.length > 0) {
        const d = ((Date.now() - telegramDate * 1000) / 1000).toFixed(1);
        console.log(`📤 שידור ל-${clients.length} לקוחות | עיכוב כולל: ${d}s`);
    }
    clients.forEach(c => c.res.write(`data: ${JSON.stringify({ type: 'news', data: newsItem })}\n\n`));
}

// ==========================================
// ליבת הפתרון: מנהל pts + getDifference מיידי
// ==========================================

// pts = "מיקום" ידוע אחרון לכל ערוץ. בלי זה לא ניתן לשאוב את ה"הפרש".
const channelPts = new Map();

// תור לטיפול בערוצים שיצאו מסנכרון - מניעת מבול בקשות מקביל
const recoveryQueue = new Set();

async function recoverChannel(client, channelId, reason) {
    const key = channelId.toString();

    // אם כבר מטפלים בערוץ הזה - לא מוסיפים כפול
    if (recoveryQueue.has(key)) return;
    recoveryQueue.add(key);

    try {
        const entity = await client.getEntity("-100" + key).catch(() =>
            client.getEntity(key).catch(() => null)
        );
        if (!entity) { recoveryQueue.delete(key); return; }

        const name = entity.title || entity.firstName || `ערוץ ${key}`;

        // getChannelDifference - זו הפקודה הנכונה לשאיבת עדכונים שהוחמצו
        const currentPts = channelPts.get(key);

        if (currentPts) {
            // יש לנו pts - נשאב את ה"הפרש" המדויק מהנקודה האחרונה הידועה
            try {
                const diff = await client.invoke(new Api.updates.GetChannelDifference({
                    channel: entity,
                    filter: new Api.ChannelMessagesFilterEmpty(),
                    pts: currentPts,
                    limit: 100,
                }));

                console.log(`🔧 [getDifference] ערוץ: ${name} | pts: ${currentPts} | סיבה: ${reason}`);

                let messages = [];
                let newPts = currentPts;

                if (diff.className === 'updates.ChannelDifference') {
                    messages = diff.newMessages || [];
                    newPts = diff.pts;
                } else if (diff.className === 'updates.ChannelDifferenceTooLong') {
                    // ערוץ עמוס מאוד - פספסנו יותר מדי, נשאב ישירות
                    messages = diff.messages || [];
                    newPts = diff.pts;
                    console.log(`⚠️ [getDifference] TooLong - שאיבה ישירה`);
                }
                // updates.ChannelDifferenceEmpty = אין חדש

                channelPts.set(key, newPts);

                for (const msg of messages) {
                    if (!msg || !msg.message) continue;
                    logLatency(name, 'recovered_getDifference', msg.date, `pts: ${currentPts}→${newPts}`);
                    const item = buildNewsItem(msg, name, key);
                    addNewsItem(item, msg.date);
                }

            } catch (diffErr) {
                // getDifference נכשל (ערוץ לא נגיש, flood) - fallback לשאיבה ישירה
                console.warn(`[getDifference] נכשל עבור ${name}: ${diffErr.message} - fallback לשאיבה ישירה`);
                await fallbackFetchMessages(client, entity, name, key);
            }

        } else {
            // אין pts שמור - שאיבה ישירה וסנכרון ראשוני
            await fallbackFetchMessages(client, entity, name, key);
        }

    } catch (e) {
        console.warn(`[recovery] שגיאה כללית עבור ${channelId}: ${e.message}`);
    } finally {
        recoveryQueue.delete(key);
    }
}

async function fallbackFetchMessages(client, entity, name, key) {
    const messages = await client.getMessages(entity, { limit: 10 });
    let newestPts = 0;
    for (const msg of messages) {
        if (!msg || !msg.message) continue;
        const age = Date.now() - msg.date * 1000;
        if (age > 10 * 60 * 1000) continue; // רק 10 דקות אחרונות
        logLatency(name, 'fallback_direct_fetch', msg.date);
        const item = buildNewsItem(msg, name, key);
        addNewsItem(item, msg.date);
        if (msg.pts && msg.pts > newestPts) newestPts = msg.pts;
    }
    // שמירת pts מהישות עצמה אם זמין
    if (entity.pts) channelPts.set(key, entity.pts);
    else if (newestPts) channelPts.set(key, newestPts);
}

// ==========================================
// בניית פריט חדשות
// ==========================================

const entityCache = new Map();

async function getEntityNameById(client, channelId) {
    const key = channelId.toString();
    if (entityCache.has(key)) return entityCache.get(key);
    try {
        let e = await client.getEntity("-100" + key).catch(() => null);
        if (!e) e = await client.getEntity(key).catch(() => null);
        const name = (e && (e.title || e.firstName)) ? (e.title || e.firstName) : `מקור (${key})`;
        entityCache.set(key, name);
        return name;
    } catch { return `מקור (${key})`; }
}

function buildNewsItem(message, channelName, channelId, isEdited = false) {
    let rawText = message.message || message.text || "";
    if (!rawText.trim()) rawText = "[מדיה]";

    const stopWords = ["להמשך קריאה", "להצטרפות", "לכל העדכונים", "כנסו",
        "לפרטים נוספים", "t.me", "chat.whatsapp.com", "לקבוצת הוואטסאפ", "לערוץ הטלגרם"];

    let filteredLines = [];
    for (let line of rawText.split('\n')) {
        if (stopWords.some(w => line.includes(w))) break;
        if (line.trim()) filteredLines.push(line);
    }

    let title = filteredLines[0] || '';
    let content = filteredLines.slice(1).join('\n') || '';
    if (title.length > 80) {
        content = title.substring(80) + (content ? '\n' + content : '');
        title = title.substring(0, 80) + '...';
    }
    if (!title && !content) return null;

    const idStr = channelId ? channelId.toString().replace('-100', '') : '';

    return {
        hash: generateHash(rawText + channelName + (message.id || '')),
        title,
        content,
        link: idStr ? `https://t.me/c/${idStr}/${message.id}` : '#',
        source: channelName + (isEdited ? ' [ערוך]' : ''),
        imageUrl: null,
        time: new Date((message.date || Math.floor(Date.now() / 1000)) * 1000).toISOString()
    };
}

function addNewsItem(item, telegramDate) {
    if (!item) return false;
    if (newsList.find(n => n.hash === item.hash)) return false;
    newsList.unshift(item);
    if (newsList.length > MAX_NEWS) newsList.pop();
    broadcast(item, telegramDate);
    return true;
}

// ==========================================
// טלגרם
// ==========================================

let isConnecting = false;

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן")) {
        console.log("דילוג - לא הוזנה Session");
        return;
    }
    if (isConnecting) return;
    isConnecting = true;

    try {
        const client = new TelegramClient(new StringSession(sessionString), apiId, apiHash, {
            connectionRetries: 10,
            retryDelay: 2000,
            autoReconnect: true,
            floodSleepThreshold: 60,
            receiveUpdates: true,
        });

        await client.connect();
        await client.getMe();
        console.log("✅ מחובר לטלגרם!");

        // טעינת דיאלוגים + שמירת pts ראשוני לכל ערוץ
        console.log("⏳ טוען דיאלוגים ומאתחל pts...");
        const dialogs = await client.getDialogs({ limit: 500 });
        for (const dialog of dialogs) {
            if (dialog.entity && dialog.entity.pts) {
                channelPts.set(dialog.entity.id.toString(), dialog.entity.pts);
            }
        }
        console.log(`✅ נטענו ${dialogs.length} דיאלוגים, ${channelPts.size} ערוצים עם pts`);

        // שמירה על חיות
        setInterval(async () => {
            try { await client.invoke(new Api.account.UpdateStatus({ offline: false })); } catch {}
        }, 30000);

        // SYNC גיבוי - רק לפני-פני אחרונה, הרבה יותר נדיר עכשיו
        setInterval(async () => {
            try {
                const freshDialogs = await client.getDialogs({ limit: 100 });
                let caught = 0;
                for (const dialog of freshDialogs) {
                    if (!dialog.entity) continue;
                    try {
                        const msgs = await client.getMessages(dialog.entity, { limit: 2 });
                        for (const msg of msgs) {
                            if (!msg?.message) continue;
                            if (Date.now() - msg.date * 1000 > 2 * 60 * 1000) continue;
                            const name = dialog.entity.title || dialog.entity.firstName || 'לא ידוע';
                            const key = dialog.entity.id?.toString();
                            const item = buildNewsItem(msg, name, key);
                            if (addNewsItem(item, msg.date)) {
                                caught++;
                                logLatency(name, 'emergency_sync_catch', msg.date, 'נתפס רק ב-sync גיבוי');
                            }
                        }
                    } catch {}
                }
                if (caught > 0) console.log(`🚨 [SYNC-BACKUP] ${caught} הודעות שה-getDifference החמיץ!`);
            } catch (e) { console.warn("sync error:", e.message); }
        }, 2 * 60 * 1000); // כל 2 דקות כגיבוי בלבד

        // --- המאזין הראשי ---
        client.addEventHandler(async (update) => {

            // ← הטריגר המרכזי: ערוץ יצא מסנכרון → שאיבה מיידית
            if (update.className === 'UpdateChannelTooLong') {
                const channelId = update.channelId?.toString();
                if (!channelId) return;
                console.log(`🚨 [TooLong] ערוץ ${channelId} - מפעיל getDifference מיידי`);
                // לא await - לא חוסמים את ה-event loop
                recoverChannel(client, channelId, 'UpdateChannelTooLong').catch(() => {});
                return;
            }

            // עדכון pts בזמן אמת מ-UpdateChannelMessageState
            if (update.className === 'UpdateChannelMessageForwards' ||
                update.className === 'UpdateChannelPts') {
                const chId = update.channelId?.toString();
                if (chId && update.pts) channelPts.set(chId, update.pts);
                return;
            }

            const validUpdateTypes = [
                'UpdateNewChannelMessage', 'UpdateEditChannelMessage',
                'UpdateNewMessage', 'UpdateEditMessage'
            ];
            if (!validUpdateTypes.includes(update.className)) return;

            const message = update.message;
            if (!message) return;

            // עדכון pts מההודעה עצמה - קריטי!
            let channelId = null;
            if (message.peerId) {
                channelId = message.peerId.channelId || message.peerId.chatId || message.peerId.userId;
            }
            if (!channelId) return;

            const key = channelId.toString();

            // שמירת pts מההודעה
            if (update.pts) channelPts.set(key, update.pts);

            const isEdited = update.className.includes('Edit');
            const channelName = await getEntityNameById(client, key);

            logLatency(channelName, 'received_by_listener', message.date,
                `${update.className} | id: ${message.id} | pts: ${update.pts || 'N/A'}`);

            const item = buildNewsItem(message, channelName, key, isEdited);
            addNewsItem(item, message.date);

        }, new Raw({}));

        isConnecting = false;

    } catch (error) {
        console.error("❌ שגיאה:", error.message);
        isConnecting = false;
        setTimeout(startTelegramClient, 30000);
    }
}

// ==========================================
// RSS
// ==========================================

async function fetchRSSData(channel) {
    try {
        const feed = await parser.parseURL(channel.url);
        feed.items.reverse().forEach(item => {
            const rawContent = item.content || item.contentSnippet || '';
            const cleanText = cheerio.load(rawContent).text().replace(/<[^>]+>/g, '').trim();
            let imageUrl = item.enclosure?.url || null;
            if (!imageUrl) {
                const m = rawContent.match(/<img[^>]+src="([^">]+)"/i);
                if (m) imageUrl = m[1];
            }
            const hash = generateHash(item.title + cleanText);
            if (newsList.find(n => n.hash === hash)) return;
            if (new Date(item.isoDate || 0).getTime() < Date.now() - 48 * 3600 * 1000) return;

            const newsItem = { hash, title: item.title, content: cleanText, link: item.link, source: channel.name, imageUrl, time: item.isoDate || new Date().toISOString() };
            newsList.unshift(newsItem);
            if (newsList.length > MAX_NEWS) newsList.pop();
            broadcast(newsItem, null);
        });
        newsList.sort((a, b) => new Date(b.time) - new Date(a.time));
    } catch (e) { console.error(`[RSS] ${channel.name}: ${e.message}`); }
}

setInterval(() => Promise.allSettled(rssChannels.map(fetchRSSData)), 60 * 1000);
Promise.allSettled(rssChannels.map(fetchRSSData));
startTelegramClient();

const RENDER_URL = process.env.RENDER_EXTERNAL_URL || '';
if (RENDER_URL) setInterval(async () => { try { await fetch(`${RENDER_URL}/ping`); } catch {} }, 10 * 60 * 1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ שרת פועל על פורט ${PORT}`));
