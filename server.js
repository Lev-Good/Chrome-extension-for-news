// שם הקובץ: server.js

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const Parser = require('rss-parser');
const cheerio = require('cheerio'); 

// ספריות טלגרם החדשות
const { Api, TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { NewMessage } = require('telegram/events');

const app = express();
app.use(cors());

const parser = new Parser({ 
    timeout: 8000,
    headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' }
});

let newsList = []; 
let clients = []; 
const MAX_NEWS = 1000; 

// --- הגדרות טלגרם של החשבון שלך ---
const apiId = parseInt(process.env.TELEGRAM_API_ID) || 31830285; // ה-API ID שלך
const apiHash = process.env.TELEGRAM_API_HASH || "04f8ab5c37f4048bdadffa771c5a4ce4"; // ה-API HASH שלך
const sessionString = process.env.TELEGRAM_SESSION || process.env.SESSION_STRING || "הכנס_כאן_את_המחרוזת_הארוכה_שקיבלת_מהסקריפט_login"; // המחרוזת מסקריפט ההתחברות או ממשתנה סביבה

// רשימת ערוצי ה-RSS בלבד (את הטלגרם השרת שואב אוטומטית ממה שאתה מנוי אליו!)
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
// חלק 2: חיבור לטלגרם בזמן אמת (Push)
// ==========================================

async function startTelegramClient() {
    if (!sessionString || sessionString.includes("הכנס_כאן") || sessionString.includes("1BAAOMTQ5LjE1NC4xNjcuOTEAUF2QhH4Ujtdpco0M2nKIcxmp0x9Bp8euotts79UwIhgMVe9xcZxLzPKkv38pR1w5EWDJis5OwD+HZfrbxnSwJNIIIUGQQY2x4OO/Mpb/a9tHQarNgsoJ3BDYBYYujOmtYeVLM+n1Z9LK/VII4fCJOgp1r5GuY7X+/y9s5kr06FvLM4V6KmSRsXjqgIu4qizkz2a6OCC16pXJgoAPg/zFt25xYgUPFHwgqGRuWNvx8pGmwptDgjJ38MVcYuO9bhjTn1JX8WRh4y+V1ZV4f07yaDbk+Sg7BGE44+GL7frx/S9andA2LSlodm2Q4cJAbz7H2MDYo0q5d3nfNPyr7KmMHMc=")) {
        console.log("דילוג על התחברות לטלגרם - לא הוזנה מחרוזת Session");
        return;
    }

    const stringSession = new StringSession(sessionString);
    const client = new TelegramClient(stringSession, apiId, apiHash, {
        connectionRetries: 5,
    });

    try {
        await client.connect();
        console.log("מחובר בהצלחה לשרתי טלגרם בזמן אמת!");

        // מאזין לכל הודעה חדשה שמגיעה לחשבון
        client.addEventHandler(async (event) => {
            const message = event.message;
            
            // מתעלם מהודעות פרטיות או קבוצות צ'אט, לוקח רק מערוצי שידור
            if (event.isPrivate) return;
            const chat = await event.getChat();
            if (!chat || !chat.broadcast) return; 

            const channelName = chat.title || "ערוץ טלגרם";
            console.log(">>> [טלגרם] התקבלה הודעה! ערוץ:", channelName, "| טקסט:", cleanText.substring(0, 40));
            let cleanText = message.message || "";
            if (!cleanText.trim()) return; // התעלמות מהודעות שהן רק תמונה ללא טקסט

            // חלוקה לכותרת ותוכן
            let lines = cleanText.split('\n').filter(l => l.trim().length > 0);
            let title = lines.length > 0 ? lines[0] : '';
            let content = lines.length > 1 ? lines.slice(1).join('\n') : '';

            if (title.length > 80) {
                content = title.substring(80) + (content ? '\n' + content : '');
                title = title.substring(0, 80) + '...';
            }

            const newsItem = {
                hash: generateHash(cleanText + channelName + message.id),
                title: title,
                content: content,
                link: `https://t.me/c/${chat.id}/${message.id}`,
                source: channelName,
                imageUrl: null, 
                time: new Date(message.date * 1000).toISOString()
            };

            newsList.unshift(newsItem);
            if (newsList.length > MAX_NEWS) newsList.pop();
            
            broadcast(newsItem); 

        }, new NewMessage({}));

    } catch (error) {
        console.error("שגיאה בחיבור לטלגרם:", error);
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
        console.error(`שגיאה בסריקת RSS מ- ${channel.name}:`, error.message);
    }
}

async function fetchAllRSS() {
    // הרצת כל הסריקות במקביל כדי שחסימה באתר אחד לא תעצור את האחרים
    Promise.allSettled(rssChannels.map(channel => fetchRSSData(channel)))
        .catch(err => console.error("שגיאה כללית בריצת ה-RSS:", err.message));
}

// הפעלת סריקת RSS כל דקה
setInterval(fetchAllRSS, 60 * 1000);
fetchAllRSS();

// הפעלת מאזין הטלגרם
startTelegramClient();

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`השרת פועל בהצלחה על פורט ${PORT}`);
});
