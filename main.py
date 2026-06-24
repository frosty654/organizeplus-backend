from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
import os, uuid, httpx, json

app = FastAPI(title="Organize+ API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
client = AsyncIOMotorClient(MONGO_URL)
db = client["organizeplus"]

# ---------- MODELOS ----------
class Goal(BaseModel):
    title: str
    valor: float
    categoria: str
    progress: int = 0

class GoalUpdate(BaseModel):
    progress: int

class ChatMessage(BaseModel):
    message: str
    history: List[dict] = []

class UserSession(BaseModel):
    name: str
    email: str

# ---------- AUTH SIMPLES (token local) ----------
sessions = {}

@app.post("/api/auth/register")
async def register(user: UserSession):
    token = str(uuid.uuid4())
    sessions[token] = {"name": user.name, "email": user.email, "id": str(uuid.uuid4())}
    return {"token": token, "user": sessions[token]}

@app.get("/api/auth/me")
async def me(authorization: str = Header(None)):
    token = authorization.replace("Bearer ", "") if authorization else ""
    if token not in sessions:
        raise HTTPException(401, "Não autenticado")
    return sessions[token]

@app.post("/api/auth/logout")
async def logout(authorization: str = Header(None)):
    token = authorization.replace("Bearer ", "") if authorization else ""
    sessions.pop(token, None)
    return {"ok": True}

# ---------- METAS ----------
def get_user_id(authorization: str = Header(None)):
    token = authorization.replace("Bearer ", "") if authorization else ""
    if token not in sessions:
        raise HTTPException(401, "Não autenticado")
    return sessions[token]["id"]

@app.get("/api/goals")
async def list_goals(user_id: str = Depends(get_user_id)):
    goals = await db.goals.find({"user_id": user_id}).to_list(100)
    for g in goals:
        g["id"] = str(g["_id"]); del g["_id"]
    return goals

@app.post("/api/goals")
async def create_goal(goal: Goal, user_id: str = Depends(get_user_id)):
    doc = {**goal.dict(), "user_id": user_id, "created_at": datetime.utcnow().isoformat()}
    result = await db.goals.insert_one(doc)
    doc["id"] = str(result.inserted_id); del doc["_id"]
    return doc

@app.patch("/api/goals/{goal_id}")
async def update_goal(goal_id: str, update: GoalUpdate, user_id: str = Depends(get_user_id)):
    from bson import ObjectId
    await db.goals.update_one(
        {"_id": ObjectId(goal_id), "user_id": user_id},
        {"$set": {"progress": update.progress}}
    )
    return {"ok": True}

@app.delete("/api/goals/{goal_id}")
async def delete_goal(goal_id: str, user_id: str = Depends(get_user_id)):
    from bson import ObjectId
    await db.goals.delete_one({"_id": ObjectId(goal_id), "user_id": user_id})
    return {"ok": True}

# ---------- COACH IA (streaming) ----------
@app.post("/api/coach/chat")
async def coach_chat(body: ChatMessage, user_id: str = Depends(get_user_id)):
    messages = body.history + [{"role": "user", "content": body.message}]

    async def generate():
        async with httpx.AsyncClient(timeout=60) as c:
            async with c.stream("POST", "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-6",
                    "max_tokens": 1000,
                    "stream": True,
                    "system": "Você é um coach financeiro especializado em finanças pessoais para brasileiros de baixa e média renda. Responda sempre em português do Brasil, de forma amigável, prática e acessível. Dê dicas concretas e motivadoras. Seja conciso (máximo 4 parágrafos). Use emojis com moderação.",
                    "messages": messages
                }
            ) as resp:
                async for line in resp.aiter_lines():
                    if line.startswith("data: "):
                        data = line[6:]
                        if data == "[DONE]": break
                        try:
                            chunk = json.loads(data)
                            if chunk.get("type") == "content_block_delta":
                                text = chunk["delta"].get("text", "")
                                if text:
                                    yield f"data: {json.dumps({'text': text})}\n\n"
                        except: pass
        yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/")
async def root():
    return {"status": "ok", "app": "Organize+"}
