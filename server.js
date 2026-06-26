// שם הקובץ: server.js
// גרסה: מערכת היברידית (Push + Active Polling) עם ריפוי עצמי

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

const apiId = parseInt(process.env.TELEGRAM_API_ID) || 31830285;
const apiHash = process.env.TELEGRAM_API_HASH || "04f8ab5c37f4048bdadffa771c5a4ce4";
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
// לוגים וסטטיסטיקה לעיכובים (Latency)
// ==========================================

const LATENCY_LOG = [];

function logLatency(source, stage, telegramTimestamp, extraInfo = '') {
    const delay = (Date.now() - telegramTimestamp * 1000) / 1000;
    const entry = {
        time: new Date().toISOString(),
        source, stage,
        serverDelaySeconds: delay.toFixed(1),
        extraInfo
    };
    LATENCY_LOG.unshift(entry);
    if (LATENCY_LOG.length > 200) LATENCY_LOG.pop();
    const icon = delay < 3 ? '🟢' : delay < 15 ? '🟡' : '🔴';
    console.log(`${icon} [${stage}] ${source} | עיכוב: ${delay.toFixed(1)}s${extraInfo ? ' | ' + extraInfo : ''}`);
}

app.get('/latency', (req, res) => res.json({
    summary: {
        total: LATENCY_LOG.length,
        avgDelay: LATENCY_LOG.length
            ? (LATENCY_LOG.reduce((s, e) => s + parseFloat(e.serverDelaySeconds), 0) / LATENCY_LOG.length).toFixed(1) + 's'
            : 'N/A',
        clients: clients.length,
        trackedChannels: [...channelState.entries()].map(([id, s]) => ({
            id, name: s.name, lastPts: s.pts, method: s.method
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
        const d = ((Date.now() - telegramDate * 1000) / 1000).toFixed(1);
        console.log(`📤 שידור ל-${clients.length} לקוחות | עיכוב כולל: ${d}s`);
    }
    clients.forEach(c => c.res.write(`data: ${JSON.stringify({ type: 'news', data: newsItem })}\n\n`));
}

// ==========================================
// מצב ערוצים - לב המערכת החדשה
// ==========================================

// שמירת מצב עבור כל ערוץ
// method: 'push' = מקבל התראות כרגיל, 'poll' = ערוץ שקט שצריך לשאוב ממנו אקטיבית
const channelState = new Map();
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

    const idClean = channelIdStr ? channelIdStr.replace('-100', '') : '';
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
// מנוע Polling אקטיבי לערוצים "אילמים"
// ==========================================

async function pollChannel(client, channelId) {
    const state = channelState.get(channelId);
    if (!state || !state.entity) return;

    try {
        const messages = await client.getMessages(state.entity, { limit: 5 });
        let newCount = 0;

        // מיון מהישן לחדש
        const sorted = [...messages].sort((a, b) => a.date - b.date);

        for (const msg of sorted) {
            if (!msg?.message) continue;

            // בדיקת pts (Position): האם ההודעה הזו חדשה ממה שכבר קראנו?
            const msgPts = msg.pts || 0;
            if (state.pts && msgPts && msgPts <= state.pts) continue;

            const age = Date.now() - msg.date * 1000;
            // שואב רק הודעות מה-3 דקות האחרונות
            if (age > 3 * 60 * 1000) continue;

            logLatency(state.name, 'active_poll', msg.date, `pts: ${state.pts}→${msgPts}`);
            const item = buildNewsItem(msg, state.name, channelId);
            if (addNewsItem(item, msg.date)) newCount++;

            // עדכון המיקום האחרון (pts)
            if (msgPts > (state.pts || 0)) state.pts = msgPts;
        }

        if (state.method === 'poll') {
            state.lastSeen = Date.now();
        }

    } catch (e) {
        // שגיאות פולינג יושתקו כדי לא להציף את הלוג
    }
}

// לולאת polling רצה כל 15 שניות רק על ערוצים שיצאו מסנכרון
function startActivePolling(client) {
    setInterval(async () => {
        const pollChannels = [...channelState.entries()]
            .filter(([, s]) => s.method === 'poll' && s.entity);

        if (pollChannels.length === 0) return;

        // משיכה מקבילית במנות של 5 כדי לא להעמיס על שרתי טלגרם ולחטוף חסימה
        const chunks = [];
        for (let i = 0; i < pollChannels.length; i += 5) {
            chunks.push(pollChannels.slice(i, i + 5));
        }

        for (const chunk of chunks) {
            await Promise.allSettled(chunk.map(([id]) => pollChannel(client, id)));
            if (chunks.length > 1) await new Promise(r => setTimeout(r, 500));
        }
    }, 15 * 1000); 
}

// ==========================================
// טלגרם - חיבור וניהול אירועים
// ==========================================

let isConnecting = false;

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן")) {
        console.log("דילוג טלגרם - לא הוזנה Session");
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
        console.log("✅ מחובר לטלגרם! (מערכת היברידית Push+Poll מופעלת)");

        // ── טעינת דיאלוגים ואתחול המצב ──
        console.log("⏳ טוען דיאלוגים וממפה ערוצים...");
        const dialogs = await client.getDialogs({ limit: 500 });

        for (const dialog of dialogs) {
            if (!dialog.entity) continue;
            const id = dialog.entity.id?.toString();
            if (!id) continue;

            const pts = dialog.dialog?.pts || dialog.pts || 0;
            const name = dialog.entity.title || dialog.entity.firstName || `ערוץ ${id}`;

            entityCache.set(id, name);
            channelState.set(id, {
                name,
                entity: dialog.entity,
                pts,
                method: 'push', // מתחיל כ-push ויעבור ל-poll לפי הצורך
                lastSeen: Date.now(),
                consecutiveMisses: 0
            });
        }

        console.log(`✅ נטענו ${dialogs.length} דיאלוגים, ${channelState.size} ערוצים אותחלו למעקב`);

        let pollCount = 0;
        for (const [id, state] of channelState.entries()) {
            if (!state.pts) {
                state.method = 'poll';
                pollCount++;
            }
        }
        console.log(`📊 ${pollCount} ערוצים ללא pts נכנסו לפולינג אקטיבי | ${channelState.size - pollCount} ב-Push`);

        // ── שומר (Watchdog): מעביר ל-poll ערוצים ששתקו מעל 2 דקות ──
        setInterval(() => {
            const now = Date.now();
            let switched = 0;
            for (const [, state] of channelState.entries()) {
                if (state.method === 'push' && now - state.lastSeen > 2 * 60 * 1000) {
                    state.method = 'poll';
                    switched++;
                }
            }
            if (switched > 0) console.log(`🔀 הכלב-שומר העביר ${switched} ערוצים ל-Polling`);
        }, 60 * 1000);

        // עדכון סטטוס אונליין לטלגרם
        setInterval(async () => {
            try { await client.invoke(new Api.account.UpdateStatus({ offline: false })); } catch {}
        }, 30000);

        // הפעלת מנוע הפולינג האקטיבי
        startActivePolling(client);

        // ── המאזין הראשי (Raw) ──
        client.addEventHandler(async (update) => {

            // אם ערוץ יצא מסנכרון לגמרי, שלח אותו מיד לפולינג
            if (update.className === 'UpdateChannelTooLong') {
                const channelId = update.channelId?.toString();
                if (!channelId) return;
                const state = channelState.get(channelId);
                if (state) {
                    console.log(`🚨 [TooLong] ערוץ ${state.name} קורס - שולח אותו ל-Polling מיידי`);
                    state.method = 'poll';
                    pollChannel(client, channelId).catch(() => {});
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

            let channelId = null;
            if (message.peerId) {
                channelId = message.peerId.channelId || message.peerId.chatId || message.peerId.userId;
            }
            if (!channelId) return;

            const key = channelId.toString();
            const isEdited = update.className.includes('Edit');

            // עדכון המצב: אם שמענו ממנו, הוא בסטטוס Push תקין!
            const state = channelState.get(key);
            if (state) {
                state.lastSeen = Date.now();
                state.consecutiveMisses = 0;
                
                // ריפוי עצמי: אם היה ב-poll וחזר לשדר - החזר ל-push וחסוך משאבים
                if (state.method === 'poll') {
                    console.log(`✅ [Recovery] ערוץ ${state.name} חזר לשדר Push כרגיל!`);
                    state.method = 'push';
                }
                if (update.pts) state.pts = update.pts;
            }

            const channelName = entityCache.get(key) || `מקור (${key})`;

            logLatency(channelName, 'received_by_listener', message.date, `${update.className} | id: ${message.id}`);

            const item = buildNewsItem(message, channelName, key, isEdited);
            addNewsItem(item, message.date);

        }, new Raw({}));

        isConnecting = false;

    } catch (error) {
        console.error("❌ שגיאה כללית בטלגרם:", error.message);
        isConnecting = false;
        setTimeout(startTelegramClient, 30000); // ניסיון מחדש אחרי 30 שניות
    }
}

// ==========================================
// RSS - מערכת גיבוי מקבילית
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
    } catch (e) { 
        console.error(`[RSS] שגיאה מ-${channel.name}: ${e.message}`); 
    }
}

setInterval(() => Promise.allSettled(rssChannels.map(fetchRSSData)), 60 * 1000);
Promise.allSettled(rssChannels.map(fetchRSSData));

// הפעלת טלגרם
startTelegramClient();

// ==========================================
// Anti-Sleep: שומר על השרת ער ב-Render (כל 10 דקות)
// ==========================================
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || '';
if (RENDER_URL) {
    setInterval(async () => { 
        try { 
            await fetch(`${RENDER_URL}/ping`); 
            console.log('💓 פעימת Anti-Sleep נשלחה בהצלחה לשרת עצמו');
        } catch {} 
    }, 10 * 60 * 1000); 
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`✅ שרת פועל על פורט ${PORT}`));
