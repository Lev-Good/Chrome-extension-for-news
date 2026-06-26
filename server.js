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

// ==========================================
// חלק 2: חיבור טלגרם עם מעקף Raw מתקדם
// ==========================================

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן")) {
        console.log("דילוג על התחברות לטלגרם - לא הוזנה מחרוזת Session");
        return;
    }

    const stringSession = new StringSession(sessionString);
    const client = new TelegramClient(stringSession, apiId, apiHash, {
        connectionRetries: 5,
    });

    try {
        await client.connect();
        await client.getMe(); 
        console.log("✅ מחובר בהצלחה לשרתי טלגרם בזמן אמת! (מעקף Raw + מנוע אונליין)");
        
        // טעינת עומק לאכלוס הזיכרון המהיר
        await client.getDialogs({ limit: 500 });
        
        // פקודת שמירה על סטטוס אונליין למניעת "הירדמות" ערוצים גדולים
        await client.invoke(new Api.account.UpdateStatus({ offline: false }));
        
        setInterval(async () => {
            try {
                await client.invoke(new Api.account.UpdateStatus({ offline: false }));
                await client.getDialogs({ limit: 15 });
            } catch (e) {}
        }, 60000);

        // --- מאזין הרדאר הגולמי והראשי ---
        client.addEventHandler(async (update) => {
            
            // 1. מערכת החייאה אוטומטית לערוצים עמוסים שיוצאים מסנכרון
            if (update.className === 'UpdateChannelTooLong') {
                const brokenChannelId = update.channelId?.toString();
                console.log(`🚨 ערוץ מזהה ${brokenChannelId} יצא מסנכרון. מתבצעת שאיבת איפוס...`);
                try {
                    await client.getMessages("-100" + brokenChannelId, { limit: 1 });
                    console.log(`✅ ערוץ ${brokenChannelId} אופס וחזר לשדר!`);
                } catch (e) {
                    // התעלמות שקטה מחסימות זמניות
                }
                return;
            }

            // 2. תפיסת כל סוגי ההודעות: חדשות/ערוכות, קבוצות/ערוצים/פרטי
            const validUpdateTypes = [
                'UpdateNewChannelMessage',
                'UpdateEditChannelMessage',
                'UpdateNewMessage',
                'UpdateEditMessage'
            ];

            if (validUpdateTypes.includes(update.className)) {
                const message = update.message;
                if (!message) return;

                // חילוץ המזהה (Peer ID) בצורה בטוחה מכל סוגי האובייקטים
                let channelId = null;
                if (message.peerId) {
                    channelId = message.peerId.channelId || message.peerId.chatId || message.peerId.userId;
                }
                if (!channelId) return;
                
                channelId = channelId.toString();

                // חילוץ טקסט חכם (כולל כיתוב למדיה)
                let rawText = message.message || message.text || "";
                if (!rawText.trim()) {
                    rawText = "[מדיה - תמונה/סרטון/סטיקר]";
                }

                // בניית השם - מנסה קודם למשוך מהזיכרון כדי לא להעמיס על השרתים
                let channelName = "מקור (" + channelId + ")";
                try {
                    let entity = await client.getEntity("-100" + channelId).catch(() => null);
                    if (!entity) entity = await client.getEntity(channelId).catch(() => null);
                    
                    if (entity && (entity.title || entity.firstName)) {
                        channelName = entity.title || entity.firstName;
                    }
                } catch (e) {}

                const isEdited = update.className.includes('Edit') ? "[ערוך]" : "";
                console.log(`\n🚀 [תפיסה ${isEdited}] מקור: ${channelName} | טקסט: ${rawText.substring(0, 50).replace(/\n/g, ' ')}`);

                // מנגנון ניקוי פרסומות ולינקים מיותרים
                const stopWords = ["להמשך קריאה", "להצטרפות", "לכל העדכונים", "כנסו", "לפרטים נוספים", "t.me", "chat.whatsapp.com", "לקבוצת הוואטסאפ", "לערוץ הטלגרם"];
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
                
                if (!title && !content) return;

                const newsItem = {
                    hash: generateHash(rawText + channelName + message.id),
                    title: title, 
                    content: content,
                    link: `https://t.me/c/${channelId.replace('-100', '')}/${message.id}`,
                    source: channelName,
                    imageUrl: null, 
                    time: new Date((message.date || Math.floor(Date.now() / 1000)) * 1000).toISOString()
                };

                // דחיפה לזיכרון השרת (ללא כפילויות)
                const exists = newsList.find(n => n.hash === newsItem.hash);
                if (!exists) {
                    newsList.unshift(newsItem);
                    if (newsList.length > MAX_NEWS) newsList.pop();
                    
                    // שידור בזמן אמת לתוסף
                    broadcast(newsItem); 
                }
            } else if (update.message) {
                // לוג מעקב למקרה שיש סוג הודעה נדיר שעדיין לא כיסינו
                console.log(`🔍 [הודעה שלא סוננה] סוג האובייקט: ${update.className}`);
            }
        }, new Raw({}));

    } catch (error) {
        console.error("שגיאה באתחול טלגרם:", error);
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

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`השרת פועל בהצלחה על פורט ${PORT}`);
});
