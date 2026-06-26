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

function logLatency(source, stage, telegramTimestamp, extraInfo = '') {
    const delay = (Date.now() - telegramTimestamp * 1000) / 1000;
    const entry = { time: new Date().toISOString(), source, stage, serverDelaySeconds: delay.toFixed(1), extraInfo };
    LATENCY_LOG.unshift(entry);
    if (LATENCY_LOG.length > 200) LATENCY_LOG.pop();
    const icon = delay < 3 ? '🟢' : delay < 15 ? '🟡' : '🔴';
    console.log(`${icon} [${stage}] ${source} | עיכוב: ${delay.toFixed(1)}s${extraInfo ? ' | ' + extraInfo : ''}`);
}

app.get('/latency', (req, res) => res.json({
    summary: { total: LATENCY_LOG.length, clients: clients.length,
        globalState: { pts: globalPts, date: globalDate, qts: globalQts }
    },
    log: LATENCY_LOG
}));

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
// מצב גלובלי - לב הפתרון החדש
// ==========================================

// pts/date/qts = "מיקום" גלובלי בתור העדכונים של טלגרם
// updates.GetDifference מחזיר את כל מה שקרה מאז הנקודה הזו
let globalPts = 0;
let globalDate = 0;
let globalQts = 0;
let isDiffRunning = false;

const entityCache = new Map();

function buildNewsItem(message, channelName, channelIdStr, isEdited = false) {
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

    const idClean = channelIdStr ? channelIdStr.toString().replace('-100', '') : '';
    return {
        hash: generateHash(rawText + channelName + (message.id || '')),
        title, content,
        link: idClean ? `https://t.me/c/${idClean}/${message.id}` : '#',
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

function getChannelName(peerId) {
    if (!peerId) return null;
    const id = (peerId.channelId || peerId.chatId || peerId.userId)?.toString();
    return id ? entityCache.get(id) || `מקור (${id})` : null;
}

function getPeerId(peerId) {
    if (!peerId) return null;
    return (peerId.channelId || peerId.chatId || peerId.userId)?.toString() || null;
}

// ==========================================
// GetDifference - שאיבת כל הפערים בבקשה אחת
// ==========================================

async function fetchDifference(client) {
    if (isDiffRunning || !globalPts) return;
    isDiffRunning = true;

    try {
        // בקשה אחת לכל הפערים - ללא לולאת ערוצים
        const diff = await client.invoke(new Api.updates.GetDifference({
            pts: globalPts,
            date: globalDate,
            qts: globalQts || 0,
        }));

        if (diff.className === 'updates.DifferenceEmpty') {
            // אין חדש - מעדכן רק את date
            if (diff.date) globalDate = diff.date;
            isDiffRunning = false;
            return;
        }

        const newMessages = diff.newMessages || [];
        const otherUpdates = diff.otherUpdates || [];
        let caught = 0;

        // עיבוד הודעות חדשות
        for (const msg of newMessages) {
            if (!msg?.message) continue;
            const channelId = getPeerId(msg.peerId);
            const name = channelId ? (entityCache.get(channelId) || `מקור (${channelId})`) : 'לא ידוע';
            logLatency(name, 'getDifference_catch', msg.date);
            const item = buildNewsItem(msg, name, channelId);
            if (addNewsItem(item, msg.date)) caught++;
        }

        // עדכון pts/date/qts מהתשובה
        if (diff.state) {
            globalPts = diff.state.pts;
            globalDate = diff.state.date;
            globalQts = diff.state.qts;
        } else if (diff.intermediateState) {
            // DifferenceTooLong - יש עוד, נמשיך
            globalPts = diff.intermediateState.pts;
            globalDate = diff.intermediateState.date;
            globalQts = diff.intermediateState.qts;
        }

        if (caught > 0) console.log(`🔄 [GetDifference] נתפסו ${caught} הודעות חדשות`);

        // אם הייתה תשובה חלקית - שאב שוב מיד
        if (diff.className === 'updates.DifferenceTooLong' || diff.intermediateState) {
            isDiffRunning = false;
            setTimeout(() => fetchDifference(client), 500);
            return;
        }

    } catch (e) {
        // FLOOD_WAIT - לא עושים כלום, רק ממתינים
        if (e.message?.includes('FLOOD_WAIT')) {
            const wait = parseInt(e.message.match(/\d+/)?.[0] || '30');
            console.warn(`⏳ [GetDifference] FloodWait ${wait}s - ממתין`);
        } else {
            console.warn(`[GetDifference] שגיאה: ${e.message}`);
        }
    }

    isDiffRunning = false;
}

// ==========================================
// טלגרם
// ==========================================

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן")) {
        console.log("דילוג - לא הוזנה Session"); return;
    }

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

        // ── שלב 1: שאיבת State גלובלי ראשוני (pts/date/qts) ──
        // updates.getState = בקשה אחת קלה, לא GetHistory
        const state = await client.invoke(new Api.updates.GetState());
        globalPts = state.pts;
        globalDate = state.date;
        globalQts = state.qts;
        console.log(`✅ State גלובלי: pts=${globalPts}, date=${globalDate}, qts=${globalQts}`);

        // ── שלב 2: טעינת שמות ערוצים בלבד (ללא GetHistory!) ──
        console.log("⏳ טוען שמות ערוצים...");
        // getDialogs ללא limit גבוה - רק לאכלוס entityCache
        const dialogs = await client.getDialogs({ limit: 200 });
        for (const dialog of dialogs) {
            if (!dialog.entity) continue;
            const id = dialog.entity.id?.toString();
            const name = dialog.entity.title || dialog.entity.firstName || `ערוץ ${id}`;
            if (id) entityCache.set(id, name);
        }
        console.log(`✅ נטענו שמות ${entityCache.size} ערוצים`);

        // ── שלב 3: GetDifference כל 30 שניות - בקשה אחת לכולם ──
        // 30 שניות = עיכוב מקסימלי. בהרבה מקרים ה-listener יתפוס לפני.
        setInterval(() => fetchDifference(client), 30 * 1000);

        // שמירה על חיות
        setInterval(async () => {
            try { await client.invoke(new Api.account.UpdateStatus({ offline: false })); } catch {}
        }, 30000);

        // ── המאזין הראשי - push בזמן אמת ──
        client.addEventHandler(async (update) => {

            // עדכון State גלובלי מכל אירוע
            if (update.pts) {
                if (update.pts > globalPts) globalPts = update.pts;
            }
            if (update.date && update.date > globalDate) globalDate = update.date;

            // UpdatesTooLong = טלגרם צועק "פספסת" → GetDifference מיידי
            if (update.className === 'UpdatesTooLong') {
                console.log('⚡ UpdatesTooLong - מפעיל GetDifference מיידי');
                fetchDifference(client).catch(() => {});
                return;
            }

            // UpdateChannelTooLong = אותו דבר לערוץ ספציפי
            if (update.className === 'UpdateChannelTooLong') {
                console.log(`⚡ UpdateChannelTooLong - מפעיל GetDifference`);
                fetchDifference(client).catch(() => {});
                return;
            }

            const validUpdateTypes = [
                'UpdateNewChannelMessage', 'UpdateEditChannelMessage',
                'UpdateNewMessage', 'UpdateEditMessage'
            ];
            if (!validUpdateTypes.includes(update.className)) return;

            const message = update.message;
            if (!message) return;

            const channelId = getPeerId(message.peerId);
            if (!channelId) return;

            const isEdited = update.className.includes('Edit');
            const channelName = entityCache.get(channelId) || `מקור (${channelId})`;

            logLatency(channelName, 'push_listener', message.date, update.className);

            const item = buildNewsItem(message, channelName, channelId, isEdited);
            addNewsItem(item, message.date);

        }, new Raw({}));

        console.log("✅ מאזין פעיל | GetDifference כל 30s");

    } catch (error) {
        console.error("❌ שגיאה:", error.message);
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
            if (!imageUrl) { const m = rawContent.match(/<img[^>]+src="([^">]+)"/i); if (m) imageUrl = m[1]; }
            const hash = generateHash(item.title + cleanText);
            if (newsList.find(n => n.hash === hash)) return;
            if (new Date(item.isoDate || 0).getTime() < Date.now() - 48 * 3600 * 1000) return;
            const newsItem = { hash, title: item.title, content: cleanText, link: item.link, source: channel.name, imageUrl, time: item.isoDate || new Date().toISOString() };
            newsList.unshift(newsItem);
            if (newsList.length > MAX_NEWS) newsList.pop();
            broadcast(newsItem, null);
        });
        newsList.sort((a, b) => new Date(b.time) - new Date(a.time));
    } catch (e) { console.error(`[RSS] שגיאה מ-${channel.name}: ${e.message}`); }
}

setInterval(() => Promise.allSettled(rssChannels.map(fetchRSSData)), 60 * 1000);
Promise.allSettled(rssChannels.map(fetchRSSData));
startTelegramClient();

const RENDER_URL = process.env.RENDER_EXTERNAL_URL || '';
if (RENDER_URL) setInterval(async () => { try { await fetch(`${RENDER_URL}/ping`); } catch {} }, 10 * 60 * 1000);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ שרת פועל על פורט ${PORT}`));
