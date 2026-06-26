// שם הקובץ: server.js

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const Parser = require('rss-parser');
const cheerio = require('cheerio'); 

// ספריות טלגרם
const { Api, TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { Raw } = require('telegram/events'); 

const app = express();
app.use(cors());

// הגדרת דפדפן פיקטיבי לעקיפת חסימות 403 ב-RSS
const parser = new Parser({ 
    timeout: 8000,
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' }
});

let newsList = []; 
let clients = []; 
const MAX_NEWS = 1000; 

// --- הגדרות טלגרם של החשבון ---
const apiId = parseInt(process.env.TELEGRAM_API_ID) || 31830285; 
const apiHash = process.env.TELEGRAM_API_HASH || "04f8ab5c37f4048bdadffa771c5a4ce4"; 
const sessionString = process.env.TELEGRAM_SESSION || process.env.SESSION_STRING || "הכנס_כאן_את_המחרוזת_הארוכה_שקיבלת_מהסקריפט_login"; 

const rssChannels = [
    { name: "JDN (אתר)", url: "https://www.jdn.co.il/feed/" },
    { name: "ערוץ 7 (אתר)", url: "https://www.inn.co.il/Rss.aspx?Category=1" },
    { name: "סרוגים (אתר)", url: "https://www.srugim.co.il/feed" },
    { name: "המחדש (אתר)", url: "https://hm-news.co.il/feed/" },
    { name: "בחדרי חרדים (אתר)", url: "https://www.bhol.co.il/rss.xml" },
    { name: "ערוץ 14 (אתר)", url: "https://www.now14.co.il/feed/" }
];

// ==========================================
// חלק 1: API וצינור SSE 
// ==========================================

app.get('/', (req, res) => {
    res.json(newsList);
});

// נקודת קצה לשמירת השרת ער (Render Anti-Sleep)
app.get('/ping', (req, res) => res.send('pong'));

app.get('/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders(); 

    const clientId = Date.now();
    clients.push({ id: clientId, res });

    res.write(`data: ${JSON.stringify({ type: 'connected' })}\n\n`);

    req.on('close', () => {
        clients = clients.filter(client => client.id !== clientId);
    });
});

// פעימת חיים לשמירת חיבורי ה-SSE פתוחים
setInterval(() => {
    clients.forEach(c => c.res.write(':\n\n'));
}, 25000);

function generateHash(text) {
    return crypto.createHash('md5').update(text).digest('hex');
}

function broadcast(newsItem) {
    clients.forEach(client => {
        client.res.write(`data: ${JSON.stringify({ type: 'news', data: newsItem })}\n\n`);
    });
}

function addNewsItem(newsItem) {
    if (!newsItem) return;
    const exists = newsList.find(n => n.hash === newsItem.hash);
    if (!exists) {
        newsList.unshift(newsItem);
        if (newsList.length > MAX_NEWS) newsList.pop();
        broadcast(newsItem);
    }
}

// ==========================================
// פונקציות עזר: מטמון שמות ועיבוד הודעות
// ==========================================

// מטמון לשמירת שמות הערוצים כדי למנוע עומס על שרתי טלגרם (מניעת חסימות)
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

// פונקציה אחידה לעיבוד וניקוי הודעות מטלגרם
function processIncomingMessage(message, channelName, isEdited = false) {
    let rawText = message.message || message.text || "";
    if (!rawText.trim()) {
        rawText = "[מדיה - תמונה/סרטון/סטיקר]";
    }

    // מנגנון ניקוי פרסומות ולינקים מיותרים
    const stopWords = [
        "להמשך קריאה", "להצטרפות", "לכל העדכונים", "כנסו", 
        "לפרטים נוספים", "t.me", "chat.whatsapp.com", 
        "לקבוצת הוואטסאפ", "לערוץ הטלגרם"
    ];
    
    let lines = rawText.split('\n');
    let filteredLines = [];
    
    for (let line of lines) {
        if (stopWords.some(word => line.includes(word))) {
            break; 
        }
        if (line.trim().length > 0) {
            filteredLines.push(line);
        }
    }

    let title = filteredLines.length > 0 ? filteredLines[0] : '';
    let content = filteredLines.length > 1 ? filteredLines.slice(1).join('\n') : '';

    // קיצור כותרת אם היא ארוכה מדי
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
        title: title, 
        content: content,
        link: channelId ? `https://t.me/c/${channelId.toString().replace('-100', '')}/${message.id}` : '#',
        source: channelName + (isEdited ? ' [ערוך]' : ''),
        imageUrl: null, 
        time: new Date((message.date || Math.floor(Date.now() / 1000)) * 1000).toISOString()
    };
}

// ==========================================
// חלק 2: חיבור טלגרם - יציב ומפוקח (Production Ready)
// ==========================================

let isConnecting = false;

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן")) {
        console.log("דילוג על התחברות לטלגרם - לא הוזנה מחרוזת Session");
        return;
    }
    
    if (isConnecting) return;
    isConnecting = true;

    try {
        const stringSession = new StringSession(sessionString);
        const client = new TelegramClient(stringSession, apiId, apiHash, {
            connectionRetries: 10,
            retryDelay: 2000,
            autoReconnect: true, // חיבור מחדש אוטומטי
            floodSleepThreshold: 60,
        });

        await client.connect();
        await client.getMe(); 
        console.log("✅ מחובר בהצלחה לטלגרם! (מעקף Raw + מנוע אונליין + Anti-Sleep)");
        
        // טעינת עומק לאכלוס הזיכרון המהיר
        console.log("⏳ טוען דיאלוגים לזיכרון...");
        await client.getDialogs({ limit: 500 });
        console.log("✅ דיאלוגים נטענו");
        
        // פקודת שמירה על סטטוס אונליין (כל 30 שניות) למניעת "הירדמות" ערוצים גדולים
        setInterval(async () => {
            try {
                await client.invoke(new Api.account.UpdateStatus({ offline: false }));
            } catch (e) {
                console.warn("keepAlive error:", e.message);
            }
        }, 30000); 

        // --- סנכרון פערים אוטומטי (Fallback Sync) מול השרתים ---
        // שואב את העדכונים מהדקות האחרונות במקרה של פספוס פוש
        setInterval(async () => {
            try {
                const dialogs = await client.getDialogs({ limit: 50 });
                for (const dialog of dialogs) {
                    if (!dialog.entity) continue;
                    try {
                        const messages = await client.getMessages(dialog.entity, { limit: 3 });
                        for (const msg of messages) {
                            if (!msg || !msg.message) continue;
                            const age = Date.now() - (msg.date * 1000);
                            if (age > 6 * 60 * 1000) continue; // מתייחס רק ל-6 דקות אחרונות

                            const name = await getEntityName(client, dialog.entity.id.toString());
                            const item = processIncomingMessage(msg, name);
                            addNewsItem(item);
                        }
                    } catch { /* התעלמות מתקלות בגישה לדיאלוג בודד */ }
                }
            } catch (e) { console.warn("sync error:", e.message); }
        }, 5 * 60 * 1000); // כל 5 דקות

        // --- מאזין הרדאר הגולמי והראשי (Raw) ---
        client.addEventHandler(async (update) => {
            
            // איפוס ערוצים שיצאו מסנכרון
            if (update.className === 'UpdateChannelTooLong') {
                const brokenChannelId = update.channelId?.toString();
                console.log(`🚨 ערוץ מזהה ${brokenChannelId} יצא מסנכרון. מתבצעת שאיבת איפוס...`);
                try {
                    const msgs = await client.getMessages("-100" + brokenChannelId, { limit: 3 });
                    for (const msg of msgs) {
                        if (!msg || !msg.message) continue;
                        const name = await getEntityName(client, brokenChannelId);
                        const item = processIncomingMessage(msg, name);
                        addNewsItem(item);
                    }
                    console.log(`✅ ערוץ ${brokenChannelId} אופס וחזר לשדר!`);
                } catch (e) {
                    // התעלמות שקטה מחסימות זמניות
                }
                return;
            }

            // תפיסת כל סוגי ההודעות הרלוונטיות
            const validUpdateTypes = [
                'UpdateNewChannelMessage',
                'UpdateEditChannelMessage',
                'UpdateNewMessage',
                'UpdateEditMessage'
            ];

            if (validUpdateTypes.includes(update.className)) {
                const message = update.message;
                if (!message) return;

                let channelId = null;
                if (message.peerId) {
                    channelId = message.peerId.channelId || message.peerId.chatId || message.peerId.userId;
                }
                if (!channelId) return;
                
                channelId = channelId.toString();

                const isEdited = update.className.includes('Edit');
                const channelName = await getEntityName(client, channelId);

                // הדפסה ללוג
                const logText = message.message || message.text || "[מדיה]";
                console.log(`\n🚀 [תפיסה ${isEdited ? 'ערוך' : 'חדש'}] מקור: ${channelName} | טקסט: ${logText.substring(0, 50).replace(/\n/g, ' ')}`);

                const item = processIncomingMessage(message, channelName, isEdited);
                addNewsItem(item);
            }
        }, new Raw({}));

        // טיפול בניתוקים כדי שנדע שהשרת שומר על חיבור פעיל
        client.addEventHandler((update) => {
            if (update.className === 'UpdateConnectionState') {
                console.log(`🔌 מצב חיבור: ${update.state}`);
            }
        }, new Raw({}));

        isConnecting = false;

    } catch (error) {
        console.error("❌ שגיאה באתחול טלגרם:", error.message);
        isConnecting = false;
        console.log("⏳ מנסה להתחבר מחדש בעוד 30 שניות...");
        setTimeout(startTelegramClient, 30000);
    }
}

// ==========================================
// חלק 3: סריקת אתרי RSS (Polling)
// ==========================================

async function fetchRSSData(channel) {
    try {
        const feed = await parser.parseURL(channel.url);
        let itemsToProcess = feed.items.map(item => {
            const rawContent = item.content || item.contentSnippet || '';
            let cleanText = cheerio.load(rawContent).text().replace(/<[^>]+>/g, '').trim();

            let imageUrl = item.enclosure ? item.enclosure.url : null;
            if (!imageUrl) {
                const imgMatch = rawContent.match(/<img[^>]+src="([^">]+)"/i);
                if (imgMatch) imageUrl = imgMatch[1];
            }
            
            return {
                title: item.title,
                content: cleanText, 
                link: item.link,
                source: channel.name,
                imageUrl: imageUrl,
                time: item.isoDate || new Date().toISOString()
            };
        });

        itemsToProcess.reverse().forEach(item => { 
            const hash = generateHash(item.title + item.content);
            const exists = newsList.find(n => n.hash === hash);
            const isTooOld = new Date(item.time).getTime() < (Date.now() - 48 * 60 * 60 * 1000);

            if (!exists && !isTooOld) {
                const newsItem = { hash, title: item.title, content: item.content, link: item.link, source: item.source, imageUrl: item.imageUrl, time: item.time };
                newsList.unshift(newsItem);
                if (newsList.length > MAX_NEWS) newsList.pop();
                broadcast(newsItem);
            }
        });
        newsList.sort((a, b) => new Date(b.time) - new Date(a.time));

    } catch (error) {
        console.error(`[RSS] שגיאה מ-${channel.name}: ${error.message}`);
    }
}

async function fetchAllRSS() {
    // הרצה במקביל למניעת עיכובים משגיאות 403
    Promise.allSettled(rssChannels.map(channel => fetchRSSData(channel)));
}

setInterval(fetchAllRSS, 60 * 1000);
fetchAllRSS();

startTelegramClient();

// ==========================================
// Anti-Sleep: שומר על השרת ער ב-Render (כל 10 דקות)
// ==========================================
const RENDER_URL = process.env.RENDER_EXTERNAL_URL || '';
if (RENDER_URL) {
    setInterval(async () => {
        try {
            await fetch(`${RENDER_URL}/ping`);
            console.log('💓 Anti-sleep ping נשלח בהצלחה לשרת עצמו');
        } catch (e) {}
    }, 10 * 60 * 1000); // כל 10 דקות
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`✅ השרת פועל בהצלחה על פורט ${PORT}`);
});
