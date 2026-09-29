// ==========================
//  CRICKET AUCTION SERVER (RENDER READY WITH REDIS)
// ==========================

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");
const fs = require("fs");
const Redis = require("ioredis");
require('dotenv').config();

const app = express();
const server = http.createServer(app);
const io = new Server(server, { 
    cors: { origin: "*" },
    pingTimeout: 60000,
    pingInterval: 25000
});

// --- REDIS SETUP ---
const redisConnection = process.env.REDIS_URL; 
const redis = redisConnection ? new Redis(redisConnection) : null;

app.use(express.static(path.join(__dirname, "public")));

let rooms = {};
let saveScheduled = false;

// --- DATA PERSISTENCE (REDIS) ---
async function loadGameData() {
    if (!redis) {
        console.log("No Redis URL found. Running in memory-only mode.");
        return;
    }
    try {
        const data = await redis.get("cricket_auction_rooms");
        if (data) {
            const loadedRooms = JSON.parse(data);
            Object.keys(loadedRooms).forEach(roomId => {
                const room = loadedRooms[roomId];
                if (room.auction && Array.isArray(room.auction.skippedBy)) {
                    room.auction.skippedBy = new Set(room.auction.skippedBy);
                } else if (room.auction) {
                    room.auction.skippedBy = new Set();
                }
                
                if (room.auction && room.auction.phase === 'RESULT' && !room.auction.resultStage) {
                    room.auction.resultStage = 'PUBLIC';
                }
                if (room.auction.phase === 'AUCTION' && room.auction.biddingOpen && !room.auction.isPaused) {
                    startAuctionTimer(roomId, room.auction.timeLeft || 15);
                }
                rooms[roomId] = room;
            });
            console.log("Game state restored from Redis.");
        }
    } catch (e) {
        console.error("Failed to load game data from Redis", e);
    }
}

function saveGameData() {
    if (saveScheduled || !redis) return;
    saveScheduled = true;
    setTimeout(async () => {
        try {
            const dataToSave = {};
            Object.keys(rooms).forEach(roomId => {
                const room = rooms[roomId];
                const cleanRoom = {
                    hostId: room.hostId,
                    hostName: room.hostName,
                    config: room.config,
                    teams: room.teams,
                    bannedUsers: room.bannedUsers || [], // Save bans
                    auction: {
                        playerPool: room.auction.playerPool,
                        currentPlayerIndex: room.auction.currentPlayerIndex,
                        currentBid: room.auction.currentBid,
                        currentBidderId: room.auction.currentBidderId,
                        biddingOpen: room.auction.biddingOpen,
                        isPaused: room.auction.isPaused,
                        phase: room.auction.phase,
                        skippedBy: Array.from(room.auction.skippedBy || []),
                        timeLeft: room.auction.timeLeft,
                        resultStage: room.auction.resultStage || null
                    },
                    lastActivity: room.lastActivity
                };
                dataToSave[roomId] = cleanRoom;
            });
            await redis.set("cricket_auction_rooms", JSON.stringify(dataToSave), "EX", 86400);
            saveScheduled = false;
        } catch (e) {
            console.error("Failed to save to Redis", e);
            saveScheduled = false;
        }
    }, 1000);
}

const socketToUserMap = {}; 
const userToSocketMap = {};

// --- RULES & UTILS ---
const MAX_SQUAD_SIZE = 18;       
const MIN_SQUAD_TO_PLAY = 15;    
const PLAYING_11_SIZE = 11;      
const MAX_OVERSEAS_SQUAD = 6;
const MAX_OVERSEAS_P11 = 4;          // (kept for reference)
const MIN_OVERSEAS_SQUAD = 5;         // must buy at least 5 overseas players
const REQUIRED_OVERSEAS_P11 = 4;      // playing XI must have exactly 4 overseas
const BID_INCREMENT = 0.25;
const BID_TIMER_RESET = 15; // seconds the clock jumps back to after every bid

function sanitizeInput(str, maxLength = 50) {
    if (!str || typeof str !== 'string') return '';
    return str.trim().slice(0, maxLength).replace(/[<>]/g, '');
}

function calculateWeightedRating(role, bat, bowl, field) {
    let rating = 0;
    const b = parseInt(bat) || 0;
    const bo = parseInt(bowl) || 0;
    const f = parseInt(field) || 0;
    if (role === "Batsman" || role === "WK") rating = (b * 0.75) + (f * 0.20) + (bo * 0.05);
    else if (role === "Bowler") rating = (bo * 0.75) + (f * 0.20) + (b * 0.05);
    else if (role === "All-Rounder") rating = (b * 0.40) + (bo * 0.40) + (f * 0.20);
    else if (role === "Wicketkeeper") rating = (b * 0.50) + (f * 0.50);
    else rating = (b + bo + f) / 3; 
    return Math.round(rating);
}

// ==========================================================================
//  WINNER-SCORING ENGINE
//  MeritScore(player) = 0.25*StatsScore + 0.40*ValueScore + 0.20*OtherScore
//  Every player in the XI also gets a flat TeamVersatilityBonus (0-15) added
//  on top, since versatility is a whole-XI property, not a per-player one.
// ==========================================================================

// --- 1. FIELD-NAME ADAPTER -------------------------------------------------
// players.json uses its own field names for these stats. Point each logical
// key below at the ACTUAL property name on your player objects and nothing
// else in this file needs to change.
const STAT_FIELDS = {
    battingAverage:   "avg",          // e.g. p.avg          -> rename to match players.json
    battingStrikeRate:"sr",           // e.g. p.sr
    bowlingEconomy:   "economy",      // e.g. p.economy
    bowlingStrikeRate:"bowlSR",       // e.g. p.bowlSR (balls per wicket)
    battingHand:      "battingHand",  // expected values: "LHB" | "RHB"
    bowlingStyle:     "bowlingStyle"  // e.g. "RF","LF","RM","OS","LS", etc. (see STYLE_BUCKET_MAP)
};

// Maps every bowling-style code you actually use in players.json to one of
// the 4 variety buckets. Add/edit entries to match your real style codes.
const STYLE_BUCKET_MAP = {
    RF: "paceRight", RFM: "paceRight", RM: "paceRight", RMF: "paceRight", "RM/RFM": "paceRight",
    LF: "paceLeft",  LFM: "paceLeft",  LM: "paceLeft",  LMF: "paceLeft",
    OS: "spinRight", OB: "spinRight",  LB: "spinRight", // off-spin / leg-break (right-arm)
    SLA: "spinLeft", LS: "spinLeft",   LWS: "spinLeft"  // left-arm orthodox / chinaman
};

// --- 2. TUNABLE KNOBS -------------------------------------------------------
const WEIGHTS = {
    stats: 0.25,
    value: 0.40,
    other: 0.20,
    versatility: 0.15   // applied as a flat bonus, not inside MeritScore
};

// Normalization bounds for turning raw stats into a 0-100 scale.
// Adjust these to whatever range is realistic for your player pool.
const STAT_BOUNDS = {
    avgMin: 15, avgMax: 55,     // batting average
    srMin: 100, srMax: 170,     // batting strike rate
    ecoMin: 5,  ecoMax: 11,     // bowling economy (lower is better)
    bsrMin: 15, bsrMax: 35      // bowling strike rate, balls/wicket (lower is better)
};

const VALUE_SLOPE = 25;                    // how sharply overpaying drags ValueScore down
const UNCAPPED_STATS_FALLBACK_FACTOR = 0.7; // players with no 4-5yr stats get SkillRating * this, as StatsScore

// --- 3. GENERIC HELPERS ------------------------------------------------------
function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }

// value -> 0-100, higher raw value = higher score
function normStat(value, min, max) {
    if (value === null || value === undefined || isNaN(value)) return null;
    return clamp((value - min) / (max - min), 0, 1) * 100;
}
// value -> 0-100, but lower raw value = higher score (economy, bowling SR)
function normStatInverted(value, min, max) {
    const n = normStat(value, min, max);
    return n === null ? null : 100 - n;
}

// --- 4. BUCKET A: StatsScore (25%) ------------------------------------------
function getStatsScore(player) {
    const role = (player.role || "").toLowerCase();
    const avg = parseFloat(player[STAT_FIELDS.battingAverage]);
    const sr  = parseFloat(player[STAT_FIELDS.battingStrikeRate]);
    const eco = parseFloat(player[STAT_FIELDS.bowlingEconomy]);
    const bsr = parseFloat(player[STAT_FIELDS.bowlingStrikeRate]);

    const battingStats = () => {
        if (isNaN(avg) || isNaN(sr)) return null;
        return 0.6 * normStat(avg, STAT_BOUNDS.avgMin, STAT_BOUNDS.avgMax)
             + 0.4 * normStat(sr,  STAT_BOUNDS.srMin,  STAT_BOUNDS.srMax);
    };
    const bowlingStats = () => {
        if (isNaN(eco) || isNaN(bsr)) return null;
        return 0.5 * normStatInverted(eco, STAT_BOUNDS.ecoMin, STAT_BOUNDS.ecoMax)
             + 0.5 * normStatInverted(bsr, STAT_BOUNDS.bsrMin, STAT_BOUNDS.bsrMax);
    };

    let raw = null;
    if (role === "batsman" || role === "wk" || role === "wicketkeeper") {
        raw = battingStats();
    } else if (role === "bowler") {
        raw = bowlingStats();
    } else if (role.includes("all")) {
        const b = battingStats();
        const bo = bowlingStats();
        if (b !== null && bo !== null) raw = 0.5 * b + 0.5 * bo;
        else raw = (b !== null) ? b : bo;
    }

    if (raw === null) {
        // No 4-5yr stats on file (typically uncapped players) -> fallback,
        // not a hard zero, so they aren't automatically torpedoed.
        const skillRating = calculateWeightedRating(player.role, player.bat, player.bowl, player.field);
        raw = skillRating * UNCAPPED_STATS_FALLBACK_FACTOR;
    }
    return clamp(raw, 0, 100);
}

// --- 5. BUCKET B: ValueScore (40%) — steal deal vs overpaid -----------------
function getValueScore(player) {
    const base = parseFloat(player.basePrice);
    const sold = parseFloat(player.soldPrice);
    if (!base || base <= 0 || isNaN(sold)) return 50; // neutral if price data is missing
    const factor = sold / base;
    return clamp(100 - (factor - 1) * VALUE_SLOPE, 0, 100);
}

// --- 6. BUCKET C: OtherScore (20%) — existing subjective skill rating -------
function getOtherScore(player) {
    return calculateWeightedRating(player.role, player.bat, player.bowl, player.field);
}

// --- 7. Per-player MeritScore (0-100) ---------------------------------------
function getMeritScore(player) {
    const stats = getStatsScore(player);
    const value = getValueScore(player);
    const other = getOtherScore(player);
    const merit = (WEIGHTS.stats * stats) + (WEIGHTS.value * value) + (WEIGHTS.other * other);
    return clamp(merit, 0, 100);
}

// --- 8. Team Versatility (15%) — computed once per submitted XI -------------
function getBowlingBucket(player) {
    const style = player[STAT_FIELDS.bowlingStyle];
    return STYLE_BUCKET_MAP[style] || null;
}

function computeTeamVersatilityScore(selectedPlayers) {
    const bowlingBuckets = new Set();
    selectedPlayers.forEach(p => {
        const role = (p.role || "").toLowerCase();
        if (role === "bowler" || role.includes("all")) {
            const bucket = getBowlingBucket(p);
            if (bucket) bowlingBuckets.add(bucket);
        }
    });
    const bowlingVarietyScore = clamp((bowlingBuckets.size / 4) * 100, 0, 100);

    const leftHandedBatters = selectedPlayers.filter(p => {
        const role = (p.role || "").toLowerCase();
        const countsAsBatter = role !== "bowler"; // bat, WK, all-rounder all count
        return countsAsBatter && p[STAT_FIELDS.battingHand] === "LHB";
    }).length;
    const battingVarietyScore = clamp((leftHandedBatters / 3) * 100, 0, 100);

    return (0.7 * bowlingVarietyScore) + (0.3 * battingVarietyScore);
}

function loadPlayerDatabase() {
  try {
    const rawData = fs.readFileSync(path.join(__dirname, "players.json"), "utf-8");
    const players = JSON.parse(rawData);
    return players.map((p, idx) => {
        const weightedRating = calculateWeightedRating(p.role, p.bat, p.bowl, p.field);
        return {
            ...p,
            id: p.id || `player_${idx}`,
            country: p.country || "India",   
            status: p.status || "Uncapped",  
            rating: weightedRating,          
            basePrice: p.basePrice || 0.2,
            img: p.img || "https://cdn-icons-png.flaticon.com/512/166/166344.png"
        };
    });
  } catch (error) {
    console.error("CRITICAL: players.json not found!", error.message);
    return []; 
  }
}

function shuffleArray(array) {
    const arr = [...array];
    for (let i = arr.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
}


// ==========================================================================
//  AUCTION ORDER (systematic sets instead of a fully random pool)
//  Capped:   Batters -> Pace Bowlers -> Spin Bowlers -> Keepers -> All-Rounders
//  Uncapped: same sequence again.
//  Players are still shuffled *within* each set so order inside a set is random.
// ==========================================================================
const AUCTION_SEQUENCE = ["Batter", "Pace Bowler", "Spin Bowler", "Wicketkeeper", "All-Rounder"];
const AUCTION_STATUS_ORDER = ["Capped", "Uncapped"];

function isSpinStyle(style) {
    const s = (style || "").toLowerCase();
    return s.includes("spin") || s.includes("break") || s.includes("orthodox") || s.includes("chinaman");
}

function getAuctionCategory(p) {
    const role = (p.role || "").toLowerCase();
    if (role.includes("wicket") || role === "wk") return "Wicketkeeper";
    if (role.includes("all")) return "All-Rounder";
    if (role === "bowler") return isSpinStyle(p.bowlingStyle) ? "Spin Bowler" : "Pace Bowler";
    return "Batter"; // Batsman + anything unrecognised
}

function buildAuctionOrder(players) {
    const ordered = [];
    AUCTION_STATUS_ORDER.forEach(status => {
        AUCTION_SEQUENCE.forEach(category => {
            const set = players.filter(p =>
                (p.status || "Uncapped") === status && getAuctionCategory(p) === category);
            shuffleArray(set).forEach(p => ordered.push({ ...p, auctionSet: `${status} ${category}s` }));
        });
    });
    // Safety net: anything with an unexpected status still gets auctioned, at the end
    const placed = new Set(ordered.map(p => p.id));
    shuffleArray(players.filter(p => !placed.has(p.id)))
        .forEach(p => ordered.push({ ...p, auctionSet: "Other" }));
    return ordered;
}

// The host is an organiser only: they are NOT stored in room.teams.
// The client just needs a lightweight identity object to initialise its UI.
function hostIdentity(room, name) {
    return { id: room.hostId, name: name || room.hostName || "Host", purse: 0, squad: [], isHost: true };
}

function updateRoomActivity(roomId) {
    if (rooms[roomId]) rooms[roomId].lastActivity = Date.now();
}

// --- GAME LOGIC ---
function countOverseas(list) {
    return (list || []).filter(p => p.country === "Overseas").length;
}

// Returns a human-readable reason if the squad fails the rules, else null.
function getDisqualificationReason(team) {
    const reasons = [];
    const n = team.squad.length;
    const os = countOverseas(team.squad);
    if (n < MIN_SQUAD_TO_PLAY) reasons.push(`bought only ${n} players (minimum ${MIN_SQUAD_TO_PLAY})`);
    if (os < MIN_OVERSEAS_SQUAD) reasons.push(`bought only ${os} overseas players (minimum ${MIN_OVERSEAS_SQUAD})`);
    return reasons.length ? reasons.join(" and ") : null;
}

function checkEliminations(room) {
    if(!room || !room.teams) return;
    let changed = false;
    Object.values(room.teams).forEach(team => {
        if (!team.isEliminated && team.purse < BID_INCREMENT) {
            const reason = getDisqualificationReason(team);
            if (reason) {
                team.isEliminated = true;
                team.eliminationReason = `Ran out of money and ${reason}.`;
                changed = true;
            }
        }
    });
    if(changed) saveGameData();
}

function endAuctionPhase(roomId) {
    const room = rooms[roomId];
    if(!room) return;
    if (room.auction.timer) {
        clearInterval(room.auction.timer);
        room.auction.timer = null;
    }
    if (room.nextPlayerTimeout) { clearTimeout(room.nextPlayerTimeout); room.nextPlayerTimeout = null; }

    // Auction over: disqualify anyone who broke the squad rules
    const disqualified = [];
    Object.values(room.teams).forEach(team => {
        if (team.isEliminated) return;
        const reason = getDisqualificationReason(team);
        if (reason) {
            team.isEliminated = true;
            team.eliminationReason = `Disqualified: ${reason}.`;
            disqualified.push({ name: team.name, reason: team.eliminationReason });
        }
    });

    room.auction.phase = "SELECTION";
    room.auction.biddingOpen = false;
    io.to(roomId).emit("teams-updated", Object.values(room.teams));
    if (disqualified.length) io.to(roomId).emit("teams-disqualified", disqualified);
    io.to(roomId).emit("start-selection-phase");
    updateRoomActivity(roomId);
    saveGameData();

    // Nobody left to pick an XI -> go straight to (hidden) results
    if (Object.values(room.teams).every(t => t.isEliminated)) calculateWinner(roomId);
}

function checkAuctionCompletion(roomId) {
    const room = rooms[roomId];
    if (!room || room.auction.phase !== "AUCTION") return;
    const teams = Object.values(room.teams);
    const activeBidders = teams.filter(t => !t.isEliminated && !t.isFinishedBidding && t.squad.length < MAX_SQUAD_SIZE);
    
    if (activeBidders.length === 0) {
        if(room.auction.timer) {
            clearInterval(room.auction.timer);
            room.auction.timer = null;
        }
        endAuctionPhase(roomId);
    }
}

function getRankings(room) {
    const teams = Object.values(room.teams).filter(t => !t.isEliminated);
    teams.sort((a, b) => b.totalScore - a.totalScore);
    return teams;
}

// Everyone has submitted -> results are computed but NOT announced.
// Stage flow: HIDDEN -> HOST (only host can see) -> PUBLIC (everyone)
function calculateWinner(roomId) {
    const room = rooms[roomId];
    if (!room) return;
    room.auction.phase = "RESULT";
    room.auction.resultStage = "HIDDEN";
    io.to(roomId).emit("results-pending", { stage: "HIDDEN" });
    updateRoomActivity(roomId);
    saveGameData();
}

function sendResultsTo(socket, roomId, userId) {
    const room = rooms[roomId];
    if (!room || room.auction.phase !== "RESULT") return;
    const stage = room.auction.resultStage || "PUBLIC";
    const isHost = room.hostId === userId;
    const teams = getRankings(room);

    if (stage === "PUBLIC") {
        socket.emit("game-over-results", { winner: teams[0], rankings: teams, preview: false });
    } else if (stage === "HOST" && isHost) {
        socket.emit("game-over-results", { winner: teams[0], rankings: teams, preview: true });
    } else {
        socket.emit("results-pending", { stage });
    }
}

function emitPublicResults(roomId) {
    const room = rooms[roomId];
    if (!room || room.auction.phase !== "RESULT") return;
    const teams = getRankings(room);
    io.to(roomId).emit("game-over-results", { winner: teams[0], rankings: teams, preview: false });
}

// --- SQUAD REPORT (host download) ---
function escHtml(v) {
    return String(v === undefined || v === null ? "" : v)
        .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
}

function generateReportHtml(roomId, room) {
    const allTeams = Object.values(room.teams);
    const ranked = allTeams
        .filter(t => !t.isEliminated && t.submitted11)
        .sort((a, b) => b.totalScore - a.totalScore);
    const rankOf = {};
    ranked.forEach((t, i) => { rankOf[t.id] = i + 1; });

    const teamsSorted = [...allTeams].sort((a, b) => (rankOf[a.id] || 999) - (rankOf[b.id] || 999));
    const generated = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });

    const teamCards = teamsSorted.map(t => {
        const xiIds = new Set((t.playing11 || []).map(p => p.id));
        const spent = (t.squad || []).reduce((sum, p) => sum + (parseFloat(p.soldPrice) || 0), 0);
        const rows = (t.squad || []).map(p => {
            const inXI = xiIds.has(p.id);
            const tag = p.id === t.captainId ? " <span class='tag c'>C</span>"
                      : p.id === t.viceCaptainId ? " <span class='tag vc'>VC</span>" : "";
            const os = p.country === "Overseas" ? " <span class='tag os'>OS</span>" : "";
            return `<tr class="${inXI ? "xi" : ""}">
                <td>${escHtml(p.name)}${tag}${os}</td>
                <td>${escHtml(p.role)}</td>
                <td class="n">${escHtml(p.rating)}</td>
                <td class="n">&#8377;${escHtml(p.basePrice)}</td>
                <td class="n">&#8377;${escHtml(p.soldPrice)}</td>
                <td class="c">${inXI ? "&#10003;" : ""}</td>
            </tr>`;
        }).join("") || `<tr><td colspan="6" class="empty">No players bought</td></tr>`;

        const status = t.isEliminated ? escHtml(t.eliminationReason || "Disqualified")
            : t.submitted11 ? `Rank #${rankOf[t.id]} &middot; ${escHtml(t.totalScore)} pts`
            : "Playing XI not submitted";

        return `<section class="team">
            <div class="thead">
                <h2>${escHtml(t.name)}</h2>
                <div class="status">${status}</div>
            </div>
            <div class="meta">Squad: ${(t.squad || []).length} &middot; Spent: &#8377;${spent.toFixed(2)}Cr &middot; Purse left: &#8377;${(parseFloat(t.purse) || 0).toFixed(2)}Cr</div>
            <table>
                <thead><tr><th>Player</th><th>Role</th><th>Rating</th><th>Base</th><th>Sold</th><th>XI</th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </section>`;
    }).join("");

    const leaderboard = ranked.length
        ? `<ol class="lb">${ranked.map(t => `<li><span>${escHtml(t.name)}</span><b>${escHtml(t.totalScore)}</b></li>`).join("")}</ol>`
        : `<p class="empty">No playing XIs submitted yet.</p>`;

    return `<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Auction Report - Room ${escHtml(roomId)}</title>
<style>
 body{font-family:system-ui,Segoe UI,Arial,sans-serif;background:#0f172a;color:#e2e8f0;margin:0;padding:24px}
 .wrap{max-width:900px;margin:0 auto}
 h1{color:#facc15;margin:0 0 4px} .sub{color:#94a3b8;font-size:13px;margin-bottom:20px}
 .box,.team{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:16px;margin-bottom:16px}
 .box h3{margin:0 0 10px;color:#facc15;font-size:15px}
 .lb{margin:0;padding-left:22px} .lb li{padding:5px 0;border-bottom:1px solid #334155} .lb li span{display:inline-block;width:70%} .lb li b{color:#4ade80}
 .thead{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px}
 .team h2{margin:0;font-size:18px} .status{color:#4ade80;font-weight:600;font-size:14px}
 .meta{color:#94a3b8;font-size:12px;margin:6px 0 10px}
 table{width:100%;border-collapse:collapse;font-size:13px}
 th{text-align:left;color:#94a3b8;font-weight:600;border-bottom:1px solid #475569;padding:6px}
 td{padding:6px;border-bottom:1px solid #334155} td.n{text-align:right} td.c{text-align:center;color:#4ade80}
 th:nth-child(n+3){text-align:right} th:last-child{text-align:center}
 tr.xi td:first-child{font-weight:700;color:#fff}
 .tag{font-size:10px;padding:1px 5px;border-radius:4px;font-weight:700}
 .tag.c{background:#facc15;color:#000}.tag.vc{background:#94a3b8;color:#000}.tag.os{background:#1d4ed8;color:#fff}.tag.host{background:#7c3aed;color:#fff}
 .empty{color:#64748b;text-align:center}
 @media print{body{background:#fff;color:#000}.box,.team{background:#fff;border-color:#ccc}}
</style></head><body><div class="wrap">
 <h1>Cricket Auction Report</h1>
 <div class="sub">Room ${escHtml(roomId)} &middot; Generated ${escHtml(generated)} IST &middot; Bold rows = Playing XI</div>
 <div class="box"><h3>Leaderboard</h3>${leaderboard}</div>
 ${teamCards}
</div></body></html>`;
}

// --- TIMERS & HOST FLOW ---
function startAuctionTimer(roomId, startTime = 15) {
    const room = rooms[roomId];
    if (!room || !room.auction) return;
    
    const auction = room.auction;
    auction.timeLeft = startTime; 
    
    if (auction.timer) clearInterval(auction.timer);
    
    auction.timer = setInterval(() => {
      auction.timeLeft--;
      io.to(roomId).emit("timer-update", auction.timeLeft);
      
      if (auction.timeLeft <= 0) {
        clearInterval(auction.timer);
        auction.timer = null;
        
        if (auction.currentBidderId) finishBidding(roomId);
        else finishPlayerUnsold(roomId);
      }
    }, 1000);
}

function finishBidding(roomId) {
    const room = rooms[roomId];
    if (!room) return;

    const auction = room.auction;
    auction.biddingOpen = false;
    const player = auction.playerPool[auction.currentPlayerIndex];
    const winnerUserId = auction.currentBidderId;
    const team = room.teams[winnerUserId];
    
    if(team) {
        const finalPrice = parseFloat(auction.currentBid);
        team.purse = parseFloat((team.purse - finalPrice).toFixed(2));
        const soldPlayer = { ...player, soldPrice: finalPrice };
        team.squad.push(soldPlayer);
        
        checkEliminations(room);
        io.to(roomId).emit("player-sold", { 
            player, price: auction.currentBid, teamName: team.name, eliminated: team.isEliminated 
        });
    } else {
        io.to(roomId).emit("player-unsold", { player });
    }

    checkAuctionCompletion(roomId);
    prepareNext(roomId);
}
  
function finishPlayerUnsold(roomId) {
    const room = rooms[roomId];
    if (!room) return; 
    room.auction.biddingOpen = false;
    const player = room.auction.playerPool[room.auction.currentPlayerIndex];
    io.to(roomId).emit("player-unsold", { player });
    prepareNext(roomId);
}

function prepareNext(roomId) {
    const room = rooms[roomId];
    if(!room || room.auction.phase !== "AUCTION") return;
    
    room.auction.currentPlayerIndex++;
    checkEliminations(room);
    io.to(roomId).emit("teams-updated", Object.values(room.teams));
    updateRoomActivity(roomId);
    
    if (room.nextPlayerTimeout) clearTimeout(room.nextPlayerTimeout);

    room.auction.biddingOpen = false;
    room.auction.isPaused = false;
    if(room.auction.timer) { clearInterval(room.auction.timer); room.auction.timer = null; }

    room.nextPlayerTimeout = setTimeout(() => {
        if (rooms[roomId] && rooms[roomId].auction.phase === "AUCTION") {
            if (room.auction.currentPlayerIndex >= room.auction.playerPool.length) {
                endAuctionPhase(roomId);
            } else {
                io.to(roomId).emit("awaiting-next-player");
            }
        }
    }, 4000);
}

loadGameData();

// --- SOCKET CONNECTION ---
io.on("connection", (socket) => {

  socket.on("rejoin-game", ({ userId, roomId }) => {
      userId = sanitizeInput(userId, 100);
      roomId = sanitizeInput(roomId, 20).toUpperCase();
      socketToUserMap[socket.id] = userId;
      userToSocketMap[userId] = socket.id;

      if (roomId && rooms[roomId]) {
          const room = rooms[roomId];

          if (room.bannedUsers && room.bannedUsers.includes(userId)) {
              return socket.emit("error-message", "You have been banned from this room.");
          }

          const team = (room.hostId === userId) ? hostIdentity(room) : room.teams[userId];
          if (team) {
              socket.join(roomId);
              socket.emit("joined-room", { roomId, team, isHost: (room.hostId === userId) });
              io.to(roomId).emit("teams-updated", Object.values(room.teams));
              
              if(room.auction.phase === "AUCTION") {
                  const player = room.auction.playerPool[room.auction.currentPlayerIndex];
                  
                  if (room.auction.biddingOpen || room.auction.isPaused) {
                      socket.emit("player-revealed", { player, currentBid: room.auction.currentBid });
                      if (!room.auction.isPaused) {
                          socket.emit("bidding-opened", { currentBid: room.auction.currentBid, timeLeft: room.auction.timeLeft });
                          if(room.auction.currentBidderId) {
                              const leader = room.teams[room.auction.currentBidderId];
                              socket.emit("bid-updated", { 
                                  currentBid: room.auction.currentBid, 
                                  bidderId: room.auction.currentBidderId, 
                                  bidderName: leader ? leader.name : "Unknown"
                              });
                          }
                          socket.emit("timer-update", room.auction.timeLeft || 15);
                      } else {
                          socket.emit("timer-paused", { timeLeft: room.auction.timeLeft });
                      }
                  } else {
                      socket.emit("awaiting-next-player");
                  }
              } else if(room.auction.phase === "SELECTION") {
                  socket.emit("start-selection-phase");
              } else if(room.auction.phase === "RESULT") {
                  sendResultsTo(socket, roomId, userId);
              }
              return;
          }
      }
      socket.emit("error-message", "Session expired or room closed.");
  });

  socket.on("create-room", ({ teamName, purse, userId }) => {
    userId = sanitizeInput(userId, 100);
    teamName = sanitizeInput(teamName, 30) || "Team";
    socketToUserMap[socket.id] = userId;
    
    const roomId = Math.random().toString(36).substr(2, 6).toUpperCase();
    const hostPurse = Math.max(50, Math.min(500, parseFloat(purse) || 100));

    let initialPool = loadPlayerDatabase();
    if(initialPool.length > 0) initialPool = buildAuctionOrder(initialPool);
    else initialPool = [{ id: "err", name: "No Players Found", role: "N/A", rating: 0, basePrice: 0 }];

    rooms[roomId] = {
      hostId: userId,
      hostName: teamName === "Team" ? "Host" : teamName,
      config: { startingPurse: hostPurse },
      bannedUsers: [], 
      teams: {},   // host is organiser only; only joining players get teams
      auction: {
        playerPool: initialPool, currentPlayerIndex: 0, currentBid: 0, currentBidderId: null, biddingOpen: false, isPaused: false, phase: "LOBBY", skippedBy: new Set(), timeLeft: 15, timer: null
      },
      lastActivity: Date.now(),
      nextPlayerTimeout: null
    };

    saveGameData();
    socket.join(roomId);
    socket.emit("room-created", { roomId, team: hostIdentity(rooms[roomId]), isHost: true });
    io.to(roomId).emit("teams-updated", Object.values(rooms[roomId].teams));
  });

  socket.on("join-room", ({ roomId, teamName, userId }) => {
    userId = sanitizeInput(userId, 100);
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    teamName = sanitizeInput(teamName, 30) || "Team";
    socketToUserMap[socket.id] = userId;
    
    const room = rooms[roomId];
    if (!room) return socket.emit("error-message", "Room not found");

    if (room.bannedUsers && room.bannedUsers.includes(userId)) {
        return socket.emit("error-message", "You have been banned from this room.");
    }

    const joiningAsHost = (userId === room.hostId);
    if (!joiningAsHost && !room.teams[userId]) {
      room.teams[userId] = { id: userId, name: teamName, purse: room.config.startingPurse, squad: [], isEliminated: false, isFinishedBidding: false, submitted11: false, totalScore: 0 };
    }

    updateRoomActivity(roomId);
    saveGameData();
    socket.join(roomId);
    socket.emit("joined-room", { roomId, team: joiningAsHost ? hostIdentity(room) : room.teams[userId], isHost: joiningAsHost });
    
    if(room.auction.phase === "AUCTION") {
        const player = room.auction.playerPool[room.auction.currentPlayerIndex];
        if (room.auction.biddingOpen || room.auction.isPaused) {
            socket.emit("player-revealed", { player, currentBid: room.auction.currentBid });
            if (!room.auction.isPaused) {
                socket.emit("bidding-opened", { currentBid: room.auction.currentBid, timeLeft: room.auction.timeLeft });
                socket.emit("timer-update", room.auction.timeLeft);
            } else {
                socket.emit("timer-paused", { timeLeft: room.auction.timeLeft });
            }
        } else {
            socket.emit("awaiting-next-player");
        }
    } else if(room.auction.phase === "SELECTION") socket.emit("start-selection-phase");
    else if (room.auction.phase === "RESULT") sendResultsTo(socket, roomId, userId);

    checkEliminations(room);
    io.to(roomId).emit("teams-updated", Object.values(room.teams));
  });

  socket.on("leave-room", ({ roomId, userId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    if(roomId && rooms[roomId]) {
        socket.leave(roomId);
    }
    delete socketToUserMap[socket.id];
  });

  socket.on("disconnect", () => delete socketToUserMap[socket.id]);

  socket.on("finish-bidding-for-me", ({ roomId, userId }) => {
      roomId = sanitizeInput(roomId, 20).toUpperCase();
      userId = sanitizeInput(userId, 100);
      const room = rooms[roomId];
      if(!room) return;
      const team = room.teams[userId];
      if(team && !team.isEliminated && !getDisqualificationReason(team)) {
          team.isFinishedBidding = true;
          updateRoomActivity(roomId);
          saveGameData();
          io.to(roomId).emit("teams-updated", Object.values(room.teams));
          checkAuctionCompletion(roomId);
      }
  });

  // --- HOST CONTROLS ---

  socket.on("start-auction", ({ roomId }) => {
    const userId = socketToUserMap[socket.id];
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const room = rooms[roomId];
    if (!room || room.hostId !== userId) return;
    if (room.auction.phase !== "LOBBY") return;
    if (Object.keys(room.teams).length === 0) {
        return socket.emit("error-message", "At least one team must join before you can start the auction.");
    }

    room.auction.phase = "AUCTION";
    updateRoomActivity(roomId);
    saveGameData();
    io.to(roomId).emit("auction-started-signal");
    io.to(roomId).emit("awaiting-next-player");
  });

  socket.on("host-reveal-next", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId) return;

    const player = room.auction.playerPool[room.auction.currentPlayerIndex];
    room.auction.currentBid = player.basePrice;
    room.auction.currentBidderId = null;
    room.auction.skippedBy = new Set();
    room.auction.biddingOpen = false;
    room.auction.timeLeft = 15;
    room.auction.isPaused = false;

    updateRoomActivity(roomId);
    saveGameData();

    io.to(roomId).emit("player-revealed", { player, currentBid: room.auction.currentBid });
  });

  socket.on("host-start-bidding", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId) return;

    const auction = room.auction;
    auction.biddingOpen = true;
    auction.isPaused = false;
    updateRoomActivity(roomId);
    saveGameData();

    io.to(roomId).emit("bidding-opened", { currentBid: auction.currentBid, timeLeft: auction.timeLeft || 15 });
    startAuctionTimer(roomId, auction.timeLeft || 15);
  });

  socket.on("host-toggle-pause", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId || room.auction.phase !== "AUCTION") return;

    const auction = room.auction;
    if (!auction.isPaused) {
        if (auction.timer) clearInterval(auction.timer);
        auction.timer = null;
        auction.isPaused = true;
        auction.biddingOpen = false;
        io.to(roomId).emit("timer-paused", { timeLeft: auction.timeLeft });
    } else {
        auction.isPaused = false;
        auction.biddingOpen = true;
        io.to(roomId).emit("timer-resumed", { timeLeft: auction.timeLeft });
        startAuctionTimer(roomId, auction.timeLeft);
    }
    updateRoomActivity(roomId);
  });

  socket.on("host-kick-team", ({ roomId, targetUserId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId || targetUserId === userId) return;

    if (room.teams[targetUserId]) {
        if (!room.bannedUsers) room.bannedUsers = [];
        if (!room.bannedUsers.includes(targetUserId)) room.bannedUsers.push(targetUserId);

        delete room.teams[targetUserId];
        const targetSocketId = userToSocketMap[targetUserId];
        if (targetSocketId && io.sockets.sockets.get(targetSocketId)) {
            io.to(targetSocketId).emit("kicked-from-room", "You were banned by the host.");
            io.sockets.sockets.get(targetSocketId).leave(roomId);
        }
        updateRoomActivity(roomId);
        saveGameData();
        io.to(roomId).emit("teams-updated", Object.values(room.teams));
        checkAuctionCompletion(roomId);
    }
  });

  socket.on("host-adjust-purse", ({ roomId, targetUserId, newPurse }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId) return;

    const team = room.teams[targetUserId];
    if (team) {
        team.purse = Math.max(0, parseFloat(parseFloat(newPurse).toFixed(2)) || 0);
        checkEliminations(room);
        updateRoomActivity(roomId);
        saveGameData();
        io.to(roomId).emit("teams-updated", Object.values(room.teams));
    }
  });

  socket.on("host-force-skip", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId || room.auction.phase !== "AUCTION") return;

    if (room.auction.timer) {
        clearInterval(room.auction.timer);
        room.auction.timer = null;
    }
    
    if (room.auction.currentBidderId) finishBidding(roomId);
    else finishPlayerUnsold(roomId);
  });

  // NEW HOST CONTROL: End Auction
  socket.on("host-end-auction", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    
    if (!room || room.hostId !== userId || room.auction.phase !== "AUCTION") return;

    endAuctionPhase(roomId);
  });

  // Host-only: step 1 of results -> only the host sees rankings
  socket.on("host-preview-results", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId || room.auction.phase !== "RESULT") return;
    if (room.auction.resultStage === "HIDDEN") {
        room.auction.resultStage = "HOST";
        saveGameData();
    }
    if (room.auction.resultStage === "HOST") sendResultsTo(socket, roomId, userId);
  });

  // Host-only: step 2 of results -> announce to everyone
  socket.on("host-reveal-results", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId || room.auction.phase !== "RESULT") return;
    if (room.auction.resultStage === "PUBLIC") return;
    room.auction.resultStage = "PUBLIC";
    updateRoomActivity(roomId);
    saveGameData();
    emitPublicResults(roomId);
  });

  // Host-only: download formatted teams/players report
  socket.on("host-download-report", ({ roomId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    const userId = socketToUserMap[socket.id];
    const room = rooms[roomId];
    if (!room || room.hostId !== userId) return;
    socket.emit("report-ready", {
        filename: `auction-report-${roomId}.html`,
        html: generateReportHtml(roomId, room)
    });
  });

  // --- BIDDING ---
  socket.on("place-bid", ({ roomId, bidAmount, userId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    userId = sanitizeInput(userId, 100);
    bidAmount = parseFloat(bidAmount);
    
    const room = rooms[roomId];
    if (!room || !room.auction.biddingOpen) return;
    
    const auction = room.auction;
    const team = room.teams[userId];
    if (!team || team.isEliminated || team.isFinishedBidding) return;
    if (team.squad.length >= MAX_SQUAD_SIZE) return;
    
    const minBid = auction.currentBid + BID_INCREMENT;
    if (bidAmount < minBid || bidAmount > team.purse) return;

    const player = auction.playerPool[auction.currentPlayerIndex];
    if (player.country === "Overseas") {
        const overseasCount = team.squad.filter(p => p.country === "Overseas").length;
        if (overseasCount >= MAX_OVERSEAS_SQUAD) return; 
    } else {
        // Buying a domestic player must still leave enough slots for the overseas minimum
        const needOS = Math.max(0, MIN_OVERSEAS_SQUAD - countOverseas(team.squad));
        const slotsAfter = MAX_SQUAD_SIZE - team.squad.length - 1;
        if (slotsAfter < needOS) return;
    }

    auction.currentBid = parseFloat(bidAmount.toFixed(2));
    auction.currentBidderId = userId;
    auction.skippedBy = new Set();
    
    updateRoomActivity(roomId);
    io.to(roomId).emit("bid-updated", { currentBid: auction.currentBid, bidderId: userId, bidderName: team.name });
    startAuctionTimer(roomId, BID_TIMER_RESET);
  });

  socket.on("skip-for-me", ({ roomId, userId }) => {
    roomId = sanitizeInput(roomId, 20).toUpperCase();
    userId = sanitizeInput(userId, 100);
    
    const room = rooms[roomId];
    if (!room || !room.auction.biddingOpen) return;
    const auction = room.auction;

    if (!auction.skippedBy.has(userId)) auction.skippedBy.add(userId);

    const teams = Object.values(room.teams);
    const activeBidders = teams.filter(t => !t.isEliminated && !t.isFinishedBidding && t.squad.length < MAX_SQUAD_SIZE);
    const requiredSkips = auction.currentBidderId ? (activeBidders.length - 1) : activeBidders.length;

    if (auction.skippedBy.size >= requiredSkips && activeBidders.length > 0) {
        if (auction.timer) clearInterval(auction.timer);
        if (auction.currentBidderId) finishBidding(roomId);
        else finishPlayerUnsold(roomId);
    }
  });

  socket.on("submit-playing-11", ({ roomId, playerIds, cId, vcId, userId }) => {
      roomId = sanitizeInput(roomId, 20).toUpperCase();
      userId = sanitizeInput(userId, 100);
      const fail = (msg) => socket.emit("submit-error", msg);
      const room = rooms[roomId];
      if(!room) return;
      const team = room.teams[userId];
      if (!team) return;
      if (room.auction.phase !== "SELECTION") return fail("Squad selection is not active.");
      if (team.isEliminated) return fail("Your team is disqualified and cannot submit.");

      if(!Array.isArray(playerIds) || new Set(playerIds).size !== PLAYING_11_SIZE)
          return fail(`Select exactly ${PLAYING_11_SIZE} players.`);
      
      const selectedPlayers = team.squad.filter(p => playerIds.includes(p.id));
      if (selectedPlayers.length !== PLAYING_11_SIZE) return fail("Invalid player selection.");
      
      const overseasInP11 = countOverseas(selectedPlayers);
      if (overseasInP11 !== REQUIRED_OVERSEAS_P11)
          return fail(`Playing XI must have exactly ${REQUIRED_OVERSEAS_P11} overseas players (you picked ${overseasInP11}).`);
      
      const captain = selectedPlayers.find(p => p.id === cId);
      const viceCaptain = selectedPlayers.find(p => p.id === vcId);
      if(!captain || !viceCaptain || cId === vcId) return fail("Pick a different Captain and Vice-Captain from your XI.");

      // Whole-XI versatility (bowling variety + LH/RH batting mix), applied
      // as an identical flat bonus to every player in this XI.
      const teamVersatilityScore = computeTeamVersatilityScore(selectedPlayers);
      const versatilityBonus = WEIGHTS.versatility * teamVersatilityScore; // 0-15

      const getEffectiveScore = (p) => getMeritScore(p) + versatilityBonus;

      const cEffRating = getEffectiveScore(captain);
      const vcEffRating = getEffectiveScore(viceCaptain);

      let score = (cEffRating * 2) + (vcEffRating * 1.5);
      const leadershipBonus = (cEffRating * 0.10) + (vcEffRating * 0.05);
      
      selectedPlayers.forEach(p => {
          if (p.id !== cId && p.id !== vcId) {
              score += (getEffectiveScore(p) + leadershipBonus);
          }
      });

      let wkCount = 0, batCount = 0, bowlCount = 0, arCount = 0;
      selectedPlayers.forEach(p => {
          const r = p.role.toLowerCase();
          if (r.includes("wicket") || r === "wk") wkCount++;
          else if (r === "batsman" || r === "bat") batCount++;
          else if (r === "bowler" || r === "bowl") bowlCount++;
          else if (r.includes("all") || r === "ar") arCount++;
      });

      let balancePenalty = 0;
      if (wkCount > 2) balancePenalty += (wkCount - 2) * 20;
      const bowlingOptions = bowlCount + arCount;
      if (bowlingOptions < 5) balancePenalty += (5 - bowlingOptions) * 25;
      if (batCount < 3) balancePenalty += (3 - batCount) * 15;

      score -= balancePenalty;
      if (score < 0) score = 0;

      team.totalScore = Math.round(score * 100) / 100;
      team.submitted11 = true;
      team.playing11 = selectedPlayers;
      team.captainId = cId;
      team.viceCaptainId = vcId;
      socket.emit("team-submitted");
      
      updateRoomActivity(roomId);
      saveGameData();

      const activeTeams = Object.values(room.teams).filter(t => !t.isEliminated);
      if(activeTeams.every(t => t.submitted11)) calculateWinner(roomId);
      else io.to(roomId).emit("teams-updated", Object.values(room.teams));
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Server running on http://localhost:${PORT}`));