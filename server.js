// server.js - גרסה עם לוגים + תיקון latency

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
// מערכת לוגים מדויקת לאבחון latency
// ==========================================

const LATENCY_LOG = [];
const MAX_LATENCY_LOG = 200;

function logLatency(source, stage, telegramTimestamp, extraInfo = '') {
    const now = Date.now();
    const telegramMs = telegramTimestamp * 1000;
    const serverDelayMs = now - telegramMs;

    const entry = {
        time: new Date(now).toISOString(),
        source,
        stage, // 'received_by_server' | 'broadcast_to_client'
        telegramTime: new Date(telegramMs).toISOString(),
        serverDelaySeconds: (serverDelayMs / 1000).toFixed(1),
        extraInfo
    };

    LATENCY_LOG.unshift(entry);
    if (LATENCY_LOG.length > MAX_LATENCY_LOG) LATENCY_LOG.pop();

    // לוג צבעוני לפי חומרה
    const delay = serverDelayMs / 1000;
    const icon = delay < 3 ? '🟢' : delay < 15 ? '🟡' : '🔴';
    console.log(`${icon} [LATENCY] ${stage} | מקור: ${source} | עיכוב מטלגרם: ${delay.toFixed(1)}s | ${extraInfo}`);

    return entry;
}

// נקודת קצה לצפייה בלוגי latency
app.get('/latency', (req, res) => {
    res.json({
        totalLogged: LATENCY_LOG.length,
        log: LATENCY_LOG,
        summary: {
            avgServerDelay: LATENCY_LOG.length
                ? (LATENCY_LOG.reduce((s, e) => s + parseFloat(e.serverDelaySeconds), 0) / LATENCY_LOG.length).toFixed(1) + 's'
                : 'N/A',
            slowest: LATENCY_LOG.reduce((max, e) => parseFloat(e.serverDelaySeconds) > parseFloat(max) ? e.serverDelaySeconds : max, '0') + 's',
            connectedClients: clients.length
        }
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
    res.setHeader('X-Accel-Buffering', 'no'); // ← קריטי: מונע buffering בשרתי proxy/nginx
    res.flushHeaders();

    const clientId = Date.now();
    clients.push({ id: clientId, res });
    console.log(`📡 לקוח התחבר ל-SSE (סה"כ: ${clients.length})`);

    res.write(`data: ${JSON.stringify({ type: 'connected', time: new Date().toISOString() })}\n\n`);

    req.on('close', () => {
        clients = clients.filter(c => c.id !== clientId);
        console.log(`📡 לקוח התנתק מ-SSE (נותרו: ${clients.length})`);
    });
});

setInterval(() => {
    clients.forEach(c => c.res.write(':\n\n'));
}, 25000);

function generateHash(text) {
    return crypto.createHash('md5').update(text).digest('hex');
}

function broadcast(newsItem, telegramDate) {
    // לוג זמן השידור ללקוחות
    if (telegramDate) {
        const broadcastDelay = ((Date.now() - telegramDate * 1000) / 1000).toFixed(1);
        if (clients.length > 0) {
            console.log(`📤 [BROADCAST] שידור ל-${clients.length} לקוחות | עיכוב כולל מטלגרם: ${broadcastDelay}s`);
        } else {
            console.log(`⚠️ [BROADCAST] אין לקוחות מחוברים - ההודעה נשמרה בלבד`);
        }
    }

    clients.forEach(client => {
        client.res.write(`data: ${JSON.stringify({ type: 'news', data: newsItem })}\n\n`);
    });
}

// ==========================================
// טלגרם - עם לוגי latency מדויקים
// ==========================================

let isConnecting = false;
const entityCache = new Map();

async function getEntityName(client, channelId) {
    if (entityCache.has(channelId)) return entityCache.get(channelId);
    try {
        let entity = await client.getEntity("-100" + channelId).catch(() => null);
        if (!entity) entity = await client.getEntity(channelId).catch(() => null);
        const name = (entity && (entity.title || entity.firstName))
            ? (entity.title || entity.firstName)
            : `מקור (${channelId})`;
        entityCache.set(channelId, name);
        return name;
    } catch {
        return `מקור (${channelId})`;
    }
}

function processIncomingMessage(message, channelName, isEdited = false) {
    let rawText = message.message || message.text || "";
    if (!rawText.trim()) rawText = "[מדיה - תמונה/סרטון/סטיקר]";

    const stopWords = [
        "להמשך קריאה", "להצטרפות", "לכל העדכונים",
        "כנסו", "לפרטים נוספים", "t.me",
        "chat.whatsapp.com", "לקבוצת הוואטסאפ", "לערוץ הטלגרם"
    ];

    let lines = rawText.split('\n');
    let filteredLines = [];
    for (let line of lines) {
        if (stopWords.some(word => line.includes(word))) break;
        if (line.trim().length > 0) filteredLines.push(line);
    }

    let title = filteredLines[0] || '';
    let content = filteredLines.slice(1).join('\n') || '';

    if (title.length > 80) {
        content = title.substring(80) + (content ? '\n' + content : '');
        title = title.substring(0, 80) + '...';
    }

    if (!title && !content) return null;

    let channelId = null;
    if (message.peerId) {
        channelId = message.peerId.channelId || message.peerId.chatId || message.peerId.userId;
    }

    return {
        hash: generateHash(rawText + channelName + (message.id || '')),
        title,
        content,
        link: channelId
            ? `https://t.me/c/${channelId.toString().replace('-100', '')}/${message.id}`
            : '#',
        source: channelName + (isEdited ? ' [ערוך]' : ''),
        imageUrl: null,
        time: new Date((message.date || Math.floor(Date.now() / 1000)) * 1000).toISOString()
    };
}

function addNewsItem(newsItem, telegramDate) {
    if (!newsItem) return false;
    const exists = newsList.find(n => n.hash === newsItem.hash);
    if (!exists) {
        newsList.unshift(newsItem);
        if (newsList.length > MAX_NEWS) newsList.pop();
        broadcast(newsItem, telegramDate);
        return true;
    }
    return false;
}

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן")) {
        console.log("דילוג על טלגרם - לא הוזנה Session");
        return;
    }
    if (isConnecting) return;
    isConnecting = true;

    try {
        const stringSession = new StringSession(sessionString);
        const client = new TelegramClient(stringSession, apiId, apiHash, {
            connectionRetries: 10,
            retryDelay: 2000,
            autoReconnect: true,
            floodSleepThreshold: 60,
            // ↓ קריטי לקבלת עדכונים מהירים
            receiveUpdates: true,
        });

        await client.connect();
        await client.getMe();
        console.log("✅ מחובר לטלגרם!");

        // טעינה ראשונית
        console.log("⏳ טוען דיאלוגים...");
        await client.getDialogs({ limit: 500 });
        console.log("✅ דיאלוגים נטענו");

        // שמירה על סטטוס אונליין
        setInterval(async () => {
            try {
                await client.invoke(new Api.account.UpdateStatus({ offline: false }));
            } catch (e) {}
        }, 30000);

        // סנכרון getDifference לתפיסת פערים (כל 3 דקות, פחות מהגרסה הקודמת)
        setInterval(async () => {
            try {
                const dialogs = await client.getDialogs({ limit: 50 });
                let caught = 0;
                for (const dialog of dialogs) {
                    if (!dialog.entity) continue;
                    try {
                        const messages = await client.getMessages(dialog.entity, { limit: 3 });
                        for (const msg of messages) {
                            if (!msg || !msg.message) continue;
                            const age = Date.now() - (msg.date * 1000);
                            if (age > 4 * 60 * 1000) continue; // 4 דקות אחרונות בלבד

                            const name = dialog.entity.title || dialog.entity.firstName || 'לא ידוע';

                            // ← לוג: האם ההודעה נתפסה ב-sync ולא ב-listener?
                            const serverDelay = age / 1000;
                            console.log(`🔄 [SYNC-CATCH] הודעה נתפסה ב-polling ולא ב-listener! עיכוב: ${serverDelay.toFixed(1)}s | מקור: ${name}`);
                            logLatency(name, 'caught_by_sync_not_listener', msg.date, `עיכוב: ${serverDelay.toFixed(1)}s`);

                            const item = processIncomingMessage(msg, name);
                            const added = addNewsItem(item, msg.date);
                            if (added) caught++;
                        }
                    } catch {}
                }
                if (caught > 0) console.log(`🔄 [SYNC] נתפסו ${caught} הודעות שהחמיץ ה-listener`);
            } catch (e) {
                console.warn("sync error:", e.message);
            }
        }, 3 * 60 * 1000);

        // --- המאזין הראשי ---
        client.addEventHandler(async (update) => {

            if (update.className === 'UpdateChannelTooLong') {
                const brokenChannelId = update.channelId?.toString();
                console.log(`🚨 ערוץ ${brokenChannelId} יצא מסנכרון`);
                try {
                    const msgs = await client.getMessages("-100" + brokenChannelId, { limit: 5 });
                    for (const msg of msgs) {
                        if (!msg || !msg.message) continue;
                        const name = await getEntityName(client, brokenChannelId);
                        logLatency(name, 'recovered_from_TooLong', msg.date);
                        const item = processIncomingMessage(msg, name);
                        addNewsItem(item, msg.date);
                    }
                } catch (e) {}
                return;
            }

            const validUpdateTypes = [
                'UpdateNewChannelMessage',
                'UpdateEditChannelMessage',
                'UpdateNewMessage',
                'UpdateEditMessage'
            ];

            if (!validUpdateTypes.includes(update.className)) return;

            const message = update.message;
            if (!message) return;

            let channelId = null;
            if (message.peerId) {
                channelId = message.peerId.channelId || message.peerId.chatId || message.peerId.userId;
            }
            if (!channelId) return;

            const isEdited = update.className.includes('Edit');
            const channelName = await getEntityName(client, channelId.toString());

            // ← לוג מרכזי: מתי ההודעה הגיעה לשרת ביחס לזמן הטלגרם
            logLatency(channelName, 'received_by_server', message.date,
                `update: ${update.className} | msg_id: ${message.id}`);

            const item = processIncomingMessage(message, channelName, isEdited);
            addNewsItem(item, message.date);

        }, new Raw({}));

        isConnecting = false;

    } catch (error) {
        console.error("❌ שגיאה בטלגרם:", error.message);
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
            let imageUrl = item.enclosure ? item.enclosure.url : null;
            if (!imageUrl) {
                const imgMatch = rawContent.match(/<img[^>]+src="([^">]+)"/i);
                if (imgMatch) imageUrl = imgMatch[1];
            }

            const hash = generateHash(item.title + cleanText);
            const exists = newsList.find(n => n.hash === hash);
            const isTooOld = new Date(item.isoDate || Date.now()).getTime() < (Date.now() - 48 * 60 * 60 * 1000);

            if (!exists && !isTooOld) {
                const newsItem = {
                    hash,
                    title: item.title,
                    content: cleanText,
                    link: item.link,
                    source: channel.name,
                    imageUrl,
                    time: item.isoDate || new Date().toISOString()
                };
                newsList.unshift(newsItem);
                if (newsList.length > MAX_NEWS) newsList.pop();
                broadcast(newsItem, null);
            }
        });
        newsList.sort((a, b) => new Date(b.time) - new Date(a.time));
    } catch (error) {
        console.error(`[RSS] שגיאה מ-${channel.name}: ${error.message}`);
    }
}

setInterval(() => Promise.allSettled(rssChannels.map(fetchRSSData)), 60 * 1000);
Promise.allSettled(rssChannels.map(fetchRSSData));

startTelegramClient();

// Anti-sleep
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || '';
if (RENDER_URL) {
    setInterval(async () => {
        try {
            await fetch(`${RENDER_URL}/ping`);
        } catch (e) {}
    }, 10 * 60 * 1000);
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`✅ השרת פועל על פורט ${PORT}`);
});
