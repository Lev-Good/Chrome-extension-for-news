// שם הקובץ: server.js

process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

const express = require('express');
const cors = require('cors');
const crypto = require('crypto');
const Parser = require('rss-parser');
const cheerio = require('cheerio'); 

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
        await client.getMe(); // קריאה הכרחית לאימות סופי מול השרת
        console.log("מחובר בהצלחה לשרתי טלגרם בזמן אמת! (מצב צינור פתוח + מנוע אונליין)");
        
        // --- פתרון הקסם: מנוע אל-כשל לסטטוס "מחובר" (Online Status) ---
        // פקודה זו צועקת לטלגרם "המסך שלי דלוק ואני קורא!", והיא תרוץ כל דקה.
        await client.invoke(new Api.account.UpdateStatus({ offline: false }));
        console.log(">>> שרתי טלגרם קיבלו פקודת 'אני מחובר'. זרם הערוצים נפתח!");
        
        setInterval(async () => {
            try {
                // דיווח אונליין קבוע כדי שטלגרם לא ירדים לנו את הערוצים!
                await client.invoke(new Api.account.UpdateStatus({ offline: false }));
                // משיכת הודעות קלילה רק בשביל לרענן את הצינור הפנימי (socket)
                await client.getDialogs({ limit: 15 });
            } catch (e) {
                // מתעלמים משגיאות שקטות
            }
        }, 60000); // הפעלה כל דקה בדיוק
        // -------------------------------------------------------------

        client.addEventHandler(async (event) => {
            const message = event.message;
            if (!message) return;

            let chat;
            try {
                chat = await event.getChat();
                if (!chat || !chat.title) {
                    chat = await client.getEntity(event.chatId);
                }
            } catch (e) {
                console.log(">>> [שגיאה] זיהוי ערוץ נכשל:", event.chatId?.toString());
            }

            const sourceName = chat?.title || chat?.firstName || chat?.username || "מקור לא ידוע";
            
            let rawText = message.text || message.message || "";
            if (!rawText.trim()) {
                rawText = "[הודעה ללא טקסט - תמונה/וידאו/סטיקר]";
            }

            console.log(">>> [טלגרם פתוח] התקבל מ:", sourceName, "| טקסט:", rawText.substring(0, 50).replace(/\n/g, ' '));

            const newsItem = {
                hash: generateHash(rawText + sourceName + message.id),
                title: sourceName, 
                content: rawText,
                link: `https://t.me/c/${event.chatId?.toString().replace('-100', '')}/${message.id}`,
                source: sourceName,
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
        // מציג רק את השם ואת קוד השגיאה במקום ערימת לוגים כדי לא להציף את המסוף
        console.error(`[RSS] שגיאה מ-${channel.name}: ${error.message}`);
    }
}

async function fetchAllRSS() {
    Promise.allSettled(rssChannels.map(channel => fetchRSSData(channel)));
}

setInterval(fetchAllRSS, 60 * 1000);
fetchAllRSS();

startTelegramClient();

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`השרת פועל בהצלחה על פורט ${PORT}`);
});
