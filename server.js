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
    const icon = delay < 5 ? '🟢' : delay < 20 ? '🟡' : '🔴';
    console.log(`${icon} [${stage}] ${source} | עיכוב: ${delay.toFixed(1)}s${extraInfo ? ' | ' + extraInfo : ''}`);
}

app.get('/latency', (req, res) => res.json({
    summary: {
        total: LATENCY_LOG.length,
        clients: clients.length,
        polledChannels: [...channelRegistry.values()].map(c => ({
            name: c.name,
            method: c.method,
            lastMsgId: c.lastMsgId,
            lastPollAgo: c.lastPollTime ? Math.round((Date.now() - c.lastPollTime) / 1000) + 's' : 'never'
        }))
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
        console.log(`📤 שידור ל-${clients.length} לקוחות | עיכוב: ${((Date.now() - telegramDate * 1000) / 1000).toFixed(1)}s`);
    }
    clients.forEach(c => c.res.write(`data: ${JSON.stringify({ type: 'news', data: newsItem })}\n\n`));
}

// ==========================================
// רישום ערוצים - מעקב לפי lastMsgId
// ==========================================

// method: 'push' = listener עובד בזמן אמת
//         'poll' = צריך שאיבה אקטיבית סדרתית
const channelRegistry = new Map();
// { name, entity, method, lastMsgId, lastPollTime }

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

// ==========================================
// מנוע Polling סדרתי - ערוץ אחד בכל פעם
// ==========================================

let pollIndex = 0;
let isPolling = false;

// שאיבת ערוץ יחיד בלבד - min_id מונע כפילויות וFloodWait
async function pollOneChannel(client) {
    if (isPolling) return;

    const pollChannels = [...channelRegistry.entries()]
        .filter(([, c]) => c.method === 'poll' && c.entity);

    if (pollChannels.length === 0) return;

    // רוטציה: כל קריאה מטפלת בערוץ הבא בתור
    pollIndex = pollIndex % pollChannels.length;
    const [channelId, state] = pollChannels[pollIndex];
    pollIndex++;

    isPolling = true;
    try {
        // min_id = קריטי! טלגרם מחזיר רק הודעות חדשות מה-id הזה ואילך
        // בלי זה - שאיבת הכל מחדש בכל פעם = FloodWait
        const msgs = await client.getMessages(state.entity, {
            limit: 5,
            min_id: state.lastMsgId || 0
        });

        if (!msgs || msgs.length === 0) {
            state.lastPollTime = Date.now();
            isPolling = false;
            return;
        }

        // מיון מישן לחדש
        const sorted = [...msgs].sort((a, b) => a.id - b.id);
        let newCount = 0;

        for (const msg of sorted) {
            if (!msg?.message) continue;
            if (msg.id <= (state.lastMsgId || 0)) continue;

            // עדכון lastMsgId תמיד, גם אם ההודעה ישנה
            if (msg.id > (state.lastMsgId || 0)) state.lastMsgId = msg.id;

            // סינון הודעות ישנות מדי (מעל 5 דקות - הפולינג רץ מהר)
            const age = Date.now() - msg.date * 1000;
            if (age > 5 * 60 * 1000) continue;

            logLatency(state.name, 'serial_poll', msg.date, `id: ${msg.id}`);
            const item = buildNewsItem(msg, state.name, channelId);
            if (addNewsItem(item, msg.date)) newCount++;
        }

        state.lastPollTime = Date.now();

    } catch (e) {
        if (e.message?.includes('FLOOD_WAIT')) {
            const wait = parseInt(e.message.match(/\d+/)?.[0] || '10');
            console.warn(`⏳ FloodWait ${wait}s על ${state.name} - מדלג`);
            // לא מחכים - פשוט מדלגים לערוץ הבא בפעם הבאה
            state.method = 'poll_paused';
            // חזרה אחרי המתנה
            setTimeout(() => { state.method = 'poll'; }, (wait + 5) * 1000);
        } else {
            console.warn(`[poll] ${state.name}: ${e.message}`);
        }
    }

    isPolling = false;
}

// הלולאה הראשית: ערוץ אחד כל 2 שניות = מחזור שלם על 33 ערוצים כל ~66 שניות
// זה בטוח מFloodWait וגם מספיק מהיר
function startSerialPolling(client) {
    setInterval(() => pollOneChannel(client), 2000);
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

        // ── טעינת דיאלוגים + lastMsgId ראשוני ──
        console.log("⏳ טוען דיאלוגים...");
        const dialogs = await client.getDialogs({ limit: 500 });

        for (const dialog of dialogs) {
            if (!dialog.entity) continue;
            const id = dialog.entity.id?.toString();
            if (!id) continue;
            const name = dialog.entity.title || dialog.entity.firstName || `ערוץ ${id}`;

            entityCache.set(id, name);

            // topMessage = ה-id של ההודעה האחרונה בדיאלוג
            // זה הנקודה שממנה נתחיל לשאוב — לא נשאב שום דבר ישן
            const lastMsgId = dialog.dialog?.topMessage || 0;

            channelRegistry.set(id, {
                name,
                entity: dialog.entity,
                // מתחיל כ-push. הwatchdog יעביר ל-poll אם לא שמענו בזמן.
                method: 'push',
                lastMsgId,
                lastPollTime: null
            });
        }

        console.log(`✅ נטענו ${channelRegistry.size} ערוצים | topMessage אותחל לכולם`);

        // ── Watchdog: ערוץ שקט > 90 שניות → poll ──
        // 90 שניות = זמן סביר שאחריו ברור שה-push לא עובד לערוץ זה
        setInterval(() => {
            const now = Date.now();
            let switched = 0;
            for (const [, state] of channelRegistry.entries()) {
                if (state.method === 'push') {
                    // בדיקה: האם שמענו מהערוץ הזה דרך ה-listener לאחרונה?
                    // נשתמש ב-lastPollTime כ"שמענו לאחרונה"
                    const silent = !state.lastPollTime || (now - state.lastPollTime > 90 * 1000);
                    if (silent) {
                        state.method = 'poll';
                        switched++;
                    }
                }
            }
            if (switched > 0) console.log(`🔀 Watchdog: ${switched} ערוצים → poll`);
        }, 90 * 1000);

        // שמירה על חיות
        setInterval(async () => {
            try { await client.invoke(new Api.account.UpdateStatus({ offline: false })); } catch {}
        }, 30000);

        // הפעלת polling סדרתי
        startSerialPolling(client);

        // ── המאזין הראשי ──
        client.addEventHandler(async (update) => {

            if (update.className === 'UpdateChannelTooLong') {
                const chId = update.channelId?.toString();
                const state = chId ? channelRegistry.get(chId) : null;
                if (state) {
                    state.method = 'poll';
                    console.log(`⚡ TooLong → ${state.name} עובר ל-poll`);
                }
                return;
            }

            const validUpdateTypes = [
                'UpdateNewChannelMessage', 'UpdateEditChannelMessage',
                'UpdateNewMessage', 'UpdateEditMessage'
            ];
            if (!validUpdateTypes.includes(update.className)) return;

            const message = update.message;
            if (!message) return;

            const peerId = message.peerId;
            const channelId = (peerId?.channelId || peerId?.chatId || peerId?.userId)?.toString();
            if (!channelId) return;

            const state = channelRegistry.get(channelId);
            const channelName = entityCache.get(channelId) || `מקור (${channelId})`;

            // ערוץ שה-listener מקבל ממנו → push, עדכון lastMsgId
            if (state) {
                state.method = 'push';
                state.lastPollTime = Date.now(); // "שמענו עכשיו"
                if (message.id > (state.lastMsgId || 0)) state.lastMsgId = message.id;
            }

            const isEdited = update.className.includes('Edit');
            logLatency(channelName, 'push_listener', message.date, update.className);

            const item = buildNewsItem(message, channelName, channelId, isEdited);
            addNewsItem(item, message.date);

        }, new Raw({}));

        console.log("✅ מאזין + polling סדרתי פעילים");

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
