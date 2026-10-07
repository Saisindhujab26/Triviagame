import asyncio, json, secrets, string, time
from aiohttp import web, WSMsgType

ROOT = __import__('pathlib').Path(__file__).parent
PORT = 3000
QUESTION_MS = 15_000
BETWEEN_MS = 3_500
QUESTIONS = [
 {'q':'What is the largest planet in our solar system?','a':['Earth','Jupiter','Saturn','Mars'],'correct':1},
 {'q':'Which language runs directly in a web browser?','a':['Python','Java','JavaScript','C++'],'correct':2},
 {'q':'How many continents are there?','a':['5','6','7','8'],'correct':2},
 {'q':'What is the chemical symbol for gold?','a':['Ag','Au','Gd','Go'],'correct':1},
 {'q':'Which ocean is the largest?','a':['Atlantic','Indian','Arctic','Pacific'],'correct':3},
]
rooms = {}
clients = {}

def make_code():
    chars=string.ascii_uppercase+string.digits
    while True:
        c=''.join(secrets.choice(chars) for _ in range(6))
        if c not in rooms: return c

def players_public(room):
    return [{'id':p['id'],'name':p['name'],'score':p['score'],'connected':p['connected'],'host':p['id']==room['host']} for p in room['players'].values()]

def leaderboard(room): return sorted(players_public(room), key=lambda p:(-p['score'], p['name'].lower()))

async def send(ws, typ, data):
    if ws and not ws.closed: await ws.send_json({'type':typ, **data})

async def broadcast(room, typ, data):
    dead=[]
    for p in room['players'].values():
        ws=p.get('ws')
        if ws and not ws.closed:
            try: await send(ws,typ,data)
            except Exception: dead.append(p['id'])
    for pid in dead:
        if pid in room['players']: room['players'][pid]['connected']=False

async def emit_state(room):
    await broadcast(room,'roomState',{'code':room['code'],'players':players_public(room),'phase':room['phase'],'questionIndex':room['index'],'total':len(QUESTIONS)})

async def finish_question(room):
    if room['phase']!='question': return
    room['phase']='result'
    q=QUESTIONS[room['index']]
    results=[]
    for p in room['players'].values():
        ans=room['answers'].get(p['id'])
        points=0; correct=False; time_ms=None
        if ans:
            time_ms=max(0,min(QUESTION_MS,ans['at']-room['started']))
            correct=ans['answer']==q['correct']
            if correct: points=max(50, round(100-(time_ms/QUESTION_MS)*50))
        p['score']+=points
        results.append({'id':p['id'],'name':p['name'],'answer':ans['answer'] if ans else None,'correct':correct,'points':points,'timeMs':time_ms})
    await broadcast(room,'result',{'correctIndex':q['correct'],'results':results,'leaderboard':leaderboard(room),'index':room['index'],'total':len(QUESTIONS)})
    async def advance():
        await asyncio.sleep(BETWEEN_MS/1000)
        room['index']+=1
        if room['index']>=len(QUESTIONS):
            room['phase']='finished'; board=leaderboard(room)
            await broadcast(room,'gameOver',{'leaderboard':board,'winner':board[0]})
        else: await send_question(room)
    room['advance_task']=asyncio.create_task(advance())

async def send_question(room):
    room['phase']='question'; room['started']=time.time()*1000; room['answers']={}
    q=QUESTIONS[room['index']]
    await broadcast(room,'question',{'index':room['index'],'total':len(QUESTIONS),'text':q['q'],'answers':q['a'],'endsAt':round(room['started']+QUESTION_MS)})
    async def timer():
        await asyncio.sleep((QUESTION_MS+100)/1000)
        await finish_question(room)
    old=room.get('timer_task')
    if old: old.cancel()
    room['timer_task']=asyncio.create_task(timer())

def leave(ws):
    pid=clients.pop(id(ws),None)
    if not pid: return None
    for room in list(rooms.values()):
        if pid in room['players']:
            room['players'][pid]['connected']=False
            if room['host']==pid:
                online=next((p['id'] for p in room['players'].values() if p['connected']),None)
                if online: room['host']=online
            return room
    return None

async def ws_handler(request):
    ws=web.WebSocketResponse(heartbeat=20); await ws.prepare(request)
    pid=secrets.token_urlsafe(10); clients[id(ws)]=pid; room=None
    try:
        async for msg in ws:
            if msg.type!=WSMsgType.TEXT: continue
            try: data=json.loads(msg.data)
            except: continue
            action=data.get('action')
            if action=='createRoom':
                if room: continue
                name=str(data.get('name') or 'Player').strip()[:18] or 'Player'; code=make_code()
                room={'code':code,'host':pid,'players':{pid:{'id':pid,'name':name,'score':0,'connected':True,'ws':ws}},'phase':'lobby','index':0,'answers':{},'started':0,'timer_task':None,'advance_task':None}
                rooms[code]=room; clients[id(ws)]=pid; room=room; ws._room=code
                await send(ws,'created',{'code':code,'id':pid}); await emit_state(room)
            elif action=='joinRoom':
                if room: continue
                code=str(data.get('code') or '').strip().upper(); r=rooms.get(code)
                if not r: await send(ws,'error',{'message':'Room not found.'}); continue
                if r['phase']!='lobby': await send(ws,'error',{'message':'That game has already started.'}); continue
                if len(r['players'])>=8: await send(ws,'error',{'message':'Room is full.'}); continue
                name=str(data.get('name') or 'Player').strip()[:18] or 'Player'; r['players'][pid]={'id':pid,'name':name,'score':0,'connected':True,'ws':ws}; room=r; ws._room=code
                await send(ws,'joined',{'code':code,'id':pid}); await emit_state(r)
            elif action=='startGame':
                if not room: await send(ws,'error',{'message':'Not in a room.'}); continue
                if room['host']!=pid: await send(ws,'error',{'message':'Only the host can start.'}); continue
                if len(room['players'])<2: await send(ws,'error',{'message':'At least 2 players are required.'}); continue
                room['index']=0
                for p in room['players'].values(): p['score']=0
                await send_question(room)
            elif action=='submitAnswer':
                if not room or room['phase']!='question': await send(ws,'error',{'message':'Answers are closed.'}); continue
                if pid in room['answers']: await send(ws,'error',{'message':'Answer already submitted.'}); continue
                try: ans=int(data.get('answer'))
                except: ans=-1
                if ans not in range(4): await send(ws,'error',{'message':'Invalid answer.'}); continue
                now=time.time()*1000
                if now>room['started']+QUESTION_MS: await send(ws,'error',{'message':'Time is up.'}); continue
                room['answers'][pid]={'answer':ans,'at':now}; await send(ws,'answerAccepted',{})
                await broadcast(room,'playerAnswered',{'id':pid})
                if len(room['answers'])==len(room['players']): await finish_question(room)
    finally:
        r=leave(ws)
        if r:
            try: await emit_state(r)
            except: pass
    return ws

async def health(request): return web.json_response({'ok':True,'rooms':len(rooms)})
app=web.Application(); app.router.add_get('/ws',ws_handler); app.router.add_get('/health',health); app.router.add_static('/',ROOT/'public',show_index=True)
web.run_app(app,host='0.0.0.0',port=PORT)
