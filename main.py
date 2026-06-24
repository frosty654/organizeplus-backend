from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import os, uuid, httpx, json

app = FastAPI(title="Organize+ API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
client = AsyncIOMotorClient(MONGO_URL)
db = client["organizeplus"]
sessions = {}

def get_token(authorization=None):
    if not authorization: raise HTTPException(401, "Não autenticado")
    token = authorization.replace("Bearer ", "")
    if token not in sessions: raise HTTPException(401, "Não autenticado")
    return token

@app.post("/api/auth/register")
async def register(body: dict):
    token = str(uuid.uuid4())
    sessions[token] = {"name": body.get("name"), "email": body.get("email"), "id": str(uuid.uuid4())}
    return {"token": token, "user": sessions[token]}

@app.get("/api/auth/me")
async def me(authorization: str = Header(None)):
    return sessions[get_token(authorization)]

@app.post("/api/auth/logout")
async def logout(authorization: str = Header(None)):
    sessions.pop(authorization.replace("Bearer ", "") if authorization else "", None)
    return {"ok": True}

@app.get("/api/goals")
async def list_goals(authorization: str = Header(None)):
    user_id = sessions[get_token(authorization)]["id"]
    goals = await db.goals.find({"user_id": user_id}).to_list(100)
    for g in goals: g["id"] = str(g["_id"]); del g["_id"]
    return goals

@app.post("/api/goals")
async def create_goal(body: dict, authorization: str = Header(None)):
    user_id = sessions[get_token(authorization)]["id"]
    doc = {"title": body.get("title"), "valor": body.get("valor", 0), "categoria": body.get("categoria"), "progress": 0, "user_id": user_id, "created_at": datetime.utcnow().isoformat()}
    result = await db.goals.insert_one(doc)
    doc["id"] = str(result.inserted_id); del doc["_id"]
    return doc

@app.patch("/api/goals/{goal_id}")
async def update_goal(goal_id: str, body: dict, authorization: str = Header(None)):
    from bson import ObjectId
    await db.goals.update_one({"_id": ObjectId(goal_id), "user_id": sessions[get_token(authorization)]["id"]}, {"$set": {"progress": body.get("progress", 0)}})
    return {"ok": True}

@app.delete("/api/goals/{goal_id}")
async def delete_goal(goal_id: str, authorization: str = Header(None)):
    from bson import ObjectId
    await db.goals.delete_one({"_id": ObjectId(goal_id), "user_id": sessions[get_token(authorization)]["id"]})
    return {"ok": True}

@app.post("/api/coach/chat")
async def coach_chat(body: dict, authorization: str = Header(None)):
    get_token(authorization)
    messages = body.get("history", []) + [{"role": "user", "content": body.get("message", "")}]
    async def generate():
        async with httpx.AsyncClient(timeout=60) as c:
            async with c.stream("POST", "https://api.anthropic.com/v1/messages", headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}, json={"model": "claude-sonnet-4-6", "max_tokens": 1000, "stream": True, "system": "Você é um coach financeiro para brasileiros. Responda em português, de forma amigável e prática.", "messages": messages}) as resp:
                async for line in resp.aiter_lines():
                    if line.startswith("data: "):
                        data = line[6:]
                        if data == "[DONE]": break
                        try:
                            chunk = json.loads(data)
                            if chunk.get("type") == "content_block_delta":
                                text = chunk["delta"].get("text", "")
                                if text: yield f"data: {json.dumps({'text': text})}\n\n"
                        except: pass
        yield "data: [DONE]\n\n"
    return StreamingResponse(generate(), media_type="text/event-stream")

@app.get("/")
async def root():
    return {"status": "ok", "app": "Organize+"}
