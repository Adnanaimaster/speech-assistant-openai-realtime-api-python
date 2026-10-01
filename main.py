import os
import json
import base64
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect, Say, Stream
from dotenv import load_dotenv

load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PORT = int(os.getenv('PORT', 5050))
TEMPERATURE = float(os.getenv('TEMPERATURE', 0.7))
SYSTEM_MESSAGE = (
    "You are Emily Smith, a friendly and professional booking assistant calling on behalf of "
    "Sherbet Electric Taxis, London. You are making an outbound phone call to a London taxi driver. "
    "Open the call with a time-of-day greeting (good morning, good afternoon, or good evening), then say: "
    "'This is Emily Smith calling from Sherbet Electric Taxis, London.' "
    "First, verify the driver's identity: ask them to confirm they drive the taxi with the registration "
    "number given to you at the start of this call. If they confirm, continue. If they say the registration "
    "is wrong or they are not the driver, politely apologise, end the call, and treat the outcome as 'wrong number'. "
    "Once verified, explain the reason for the call: 'I am calling because the current advert on your taxi "
    "has expired, and we would like to book you in for an advert change.' "
    "IDENTITY: Never claim to be a real person. If the driver asks whether you are a real person, an AI, "
    "or a robot, always answer honestly: 'I am an automated agent working with Sherbet Electric Taxis, "
    "calling to book drivers in for an advert change.' Then continue the call naturally. "
    "HUMAN REQUESTS: If the driver asks to speak to a human or a real person, do not pretend you can transfer "
    "them. Tell them: 'Of course - I will pass this on to the team and someone from Sherbet will call you "
    "back.' Treat the outcome as 'human callback requested' and note clearly in the call summary that the "
    "driver asked to speak to a human, so the team can follow up. "
    "SLOTS: At the start of this call you may be given a list of available fitting slots, each with a date, "
    "time, and location. These are the ONLY slots that exist - never invent, guess, or assume any other date, "
    "time, or location, even if the driver suggests one. Offer the driver ALL of the provided slots (up to "
    "four), presented as a genuine choice across different days and locations - never offer just one slot "
    "when more are available, and never repeat or push a single option. If the driver hesitates or asks for "
    "other options, be flexible: offer alternative days, times, or locations from the list, and ask what "
    "would suit them best. "
    "LOCATION AVAILABILITY: Fittings currently take place only at Tiago and Kew. Camden is closed for "
    "renovation until the end of next year - we are NOT booking any appointments at Camden, so never offer "
    "it; if the driver asks about Camden, explain it is closed for renovation until the end of next year. "
    "Frank has no slots at the moment - never offer it; if the driver asks, say there are no dates available "
    "there currently. Never offer or invent a slot at Camden or Frank. "
    "LOCATIONS AND ADDRESSES (use these exact details): "
    "Camden: 2 Parkhurst Road, Camden, London, postcode N7 0SF. "
    "Kew: Unit 4A, Kew Bridge Distribution Centre, Lionel Road South, Brentford, postcode TW8 9QR. "
    "Tiago: Unit 18, Enterprise Row, Rangemoor Road, Tottenham, London, postcode N15 4LU. "
    "Frank: address to be confirmed - if asked, say the full address will be in the confirmation SMS. "
    "If the driver asks for an address, answer with the POSTCODE first, then the street address if they want "
    "more detail. Never invent or guess an address, date, or time. "
    "BOOKING: If the driver accepts a slot, repeat the chosen date, time, and location back to confirm it, "
    "then tell them: 'You are all booked in. We will send you a confirmation SMS shortly with the full details, "
    "the address, and the documents you need to bring.' Thank them warmly. After a booking is confirmed the "
    "call is NOT over yet - you must still deliver your goodbye exchange as described in ENDING THE CALL: "
    "one short closing sentence, wait for the driver's reply or a moment of silence, and only then end the "
    "call. NEVER hang up while you are confirming the booking or immediately after the confirmation - the "
    "driver must hear the whole confirmation and the goodbye. "
    "NO SLOTS: If no slot list was provided to you at the start of this call, you must NOT offer or mention "
    "any specific date, time, or location - not even approximately, and not at Tiago, Kew, Camden, or Frank. "
    "Instead, tell the driver we are confirming the fitting diary and will call back or text shortly with "
    "dates, and treat the outcome as 'no slots available'. "
    "BOOKING CLOSE-OUT (mandatory, every time a slot is confirmed): after the driver accepts a slot, you "
    "MUST say ALL of the following, in order, before the call ends: (1) repeat the chosen date, time, and "
    "location to confirm it; (2) 'You are all booked in. We will send you a confirmation SMS shortly with the "
    "full details, the address, and the documents you need to bring.'; (3) thank them warmly; (4) one short "
    "closing sentence and goodbye. Never confirm a slot and then fall silent or hang up - the confirmation "
    "SMS promise and the goodbye are part of every booking. "
    "If none of the offered slots suit the driver, apologise, tell them we will call back another time with "
    "more dates, and treat the outcome as 'callback requested'. "
    "If the driver declines the advert change entirely, accept gracefully and treat the outcome as 'declined'. "
    "ENDING THE CALL: end_call hangs up the phone line instantly, so it MUST be the very last thing you do. "
    "Never use end_call in the middle of the conversation - if the driver still has something to say, keep "
    "talking. Only end the call when a final outcome has been reached (booking confirmed, callback promised, "
    "declined, wrong number, or no slots available). When that happens: first say ONE short closing sentence "
    "(for example 'Thank you, have a great day, goodbye'), then WAIT for the driver's reply or a moment of "
    "silence, and only THEN use end_call. Never use end_call in the same turn as your goodbye or your "
    "booking confirmation, and never use it while the driver is still speaking or might respond. If the "
    "driver themselves says goodbye, reply briefly and then use end_call. "
    "Keep your responses short, natural, and conversational - this is a phone call. Never invent dates, times, "
    "or locations; only offer slots from the list given to you. Never ask for payment, personal documents, or "
    "any details beyond confirming the registration and the chosen slot. Always speak in a clear, warm British "
    "English manner."
)
VOICE = 'shimmer'
LOG_EVENT_TYPES = [
    'error', 'response.content.done', 'rate_limits.updated',
    'response.done', 'input_audio_buffer.committed',
    'input_audio_buffer.speech_stopped', 'input_audio_buffer.speech_started',
    'session.created', 'session.updated'
]
SHOW_TIMING_MATH = False

app = FastAPI()

if not OPENAI_API_KEY:
    raise ValueError('Missing the OpenAI API key. Please set it in the .env file.')

@app.get("/", response_class=JSONResponse)
async def index_page():
    return {"message": "Twilio Media Stream Server is running!"}

@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    """Handle incoming call and return TwiML response to connect to Media Stream."""
    response = VoiceResponse()
    host = request.url.hostname
    connect = Connect()
    connect.stream(url=f'wss://{host}/media-stream')
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")

def build_call_context(call_context: dict) -> str:
    """Turn the per-call parameters into a briefing line for Emily."""
    driver = call_context.get('driver', '').strip()
    reg = call_context.get('reg', '').strip()
    advert = call_context.get('advert', '').strip()
    slots = call_context.get('slots', '').strip()
    parts = ["Per-call details for this specific call (use these, never invent others):"]
    if driver:
        parts.append(f"- Driver name: {driver}. Greet them by first name.")
    else:
        parts.append("- Driver name unknown. Ask who you are speaking with.")
    if reg:
        parts.append(f"- Taxi registration to verify: {reg}.")
    else:
        parts.append("- No registration supplied. Ask the driver to confirm their taxi registration.")
    if advert:
        parts.append(f"- Their current (expired) advert/campaign: {advert}.")
    if slots:
        parts.append(f"- Available fitting slots for this driver (offer ALL of these, and ONLY these): {slots}.")
    else:
        parts.append("- NO available slots for this call. Follow the NO SLOTS rule: offer nothing.")
    return "\n".join(parts)

@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    """Handle WebSocket connections between Twilio and OpenAI."""
    print("Client connected")
    await websocket.accept()

    async with websockets.connect(
        f"wss://api.openai.com/v1/realtime?model=gpt-realtime&temperature={TEMPERATURE}",
        additional_headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}"
        }
    ) as openai_ws:
        # Per-call context arrives as <Parameter> entries on the Twilio stream
        # 'start' event; we initialise the OpenAI session only after reading it.
        call_context = {}

        # Connection specific state
        stream_sid = None
        latest_media_timestamp = 0
        last_assistant_item = None
        mark_queue = []
        response_start_timestamp_twilio = None
        session_ready = asyncio.Event()
        # Inactivity watchdog state: if the line is silent for too long (dead air
        # after an interrupted turn, or the driver listening quietly), Emily
        # re-engages instead of the call dying in silence.
        last_activity_ts = asyncio.get_event_loop().time()
        inactivity_nudges = 0
        response_in_progress = False
        caller_speaking = False
        inactivity_timeout = 12.0  # seconds of dead air before Emily nudges
        max_inactivity_nudges = 2  # after the 2nd unanswered nudge she wraps up

        def note_activity():
            nonlocal last_activity_ts, inactivity_nudges
            last_activity_ts = asyncio.get_event_loop().time()

        async def inactivity_watchdog():
            """Keep the call alive: if nobody speaks for a while, Emily checks in;
            if the driver stays silent after nudges, she wraps up and hangs up.
            The watchdog NEVER fires while Emily is generating or playing audio or
            while the caller is speaking - a nudge must never talk over anyone."""
            nonlocal inactivity_nudges, response_in_progress
            try:
                while True:
                    await asyncio.sleep(1)
                    if not session_ready.is_set() or response_in_progress or caller_speaking:
                        continue
                    silent_for = asyncio.get_event_loop().time() - last_activity_ts
                    if silent_for < inactivity_timeout:
                        continue
                    inactivity_nudges += 1
                    print(f"Inactivity watchdog: {silent_for:.0f}s of silence, nudge {inactivity_nudges}")
                    if inactivity_nudges <= max_inactivity_nudges:
                        prompt_text = (
                            "[system: the driver has been silent for a while. In ONE short sentence, "
                            "politely check they are still there and re-ask your last question, for example "
                            "'Hello, are you still there?' Do not repeat the whole offer.]"
                        )
                    else:
                        prompt_text = (
                            "[system: the driver has stayed silent after repeated attempts. Say one short "
                            "polite closing sentence (for example 'I seem to have lost you - we will call "
                            "back another time. Goodbye.'), then use end_call.]"
                        )
                    try:
                        await openai_ws.send(json.dumps({
                            "type": "conversation.item.create",
                            "item": {
                                "type": "message",
                                "role": "user",
                                "content": [{"type": "input_text", "text": prompt_text}]
                            }
                        }))
                        await openai_ws.send(json.dumps({"type": "response.create"}))
                    except Exception as e:
                        print(f"Inactivity watchdog failed to send nudge: {e}")
                    note_activity()
                    if inactivity_nudges > max_inactivity_nudges:
                        # Give her the closing sentence, then the watchdog's job is done
                        await asyncio.sleep(inactivity_timeout)
                        return
            except asyncio.CancelledError:
                pass

        async def receive_from_twilio():
            """Receive audio data from Twilio and send it to the OpenAI Realtime API."""
            nonlocal stream_sid, latest_media_timestamp, last_assistant_item, response_start_timestamp_twilio
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)
                    if data['event'] == 'media' and openai_ws.state.name == 'OPEN':
                        latest_media_timestamp = int(data['media']['timestamp'])
                        audio_append = {
                            "type": "input_audio_buffer.append",
                            "audio": data['media']['payload']
                        }
                        await openai_ws.send(json.dumps(audio_append))
                    elif data['event'] == 'start':
                        stream_sid = data['start']['streamSid']
                        custom = data['start'].get('customParameters', {}) or {}
                        call_context.update(custom)
                        print(f"Incoming stream has started {stream_sid} with context {call_context}")
                        response_start_timestamp_twilio = None
                        latest_media_timestamp = 0
                        last_assistant_item = None
                        if not session_ready.is_set():
                            await initialize_session(openai_ws, build_call_context(call_context))
                            session_ready.set()
                    elif data['event'] == 'mark':
                        if mark_queue:
                            mark_queue.pop(0)
            except WebSocketDisconnect:
                print("Client disconnected.")
                if openai_ws.state.name == 'OPEN':
                    await openai_ws.close()

        async def send_to_twilio():
            """Receive events from the OpenAI Realtime API, send audio back to Twilio."""
            nonlocal stream_sid, last_assistant_item, response_start_timestamp_twilio, response_in_progress, caller_speaking
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)
                    if response['type'] in LOG_EVENT_TYPES:
                        print(f"Received event: {response['type']}", response)

                    if response.get('type') == 'response.output_audio.delta' and 'delta' in response:
                        response_in_progress = True
                        note_activity()
                        audio_payload = base64.b64encode(base64.b64decode(response['delta'])).decode('utf-8')
                        audio_delta = {
                            "event": "media",
                            "streamSid": stream_sid,
                            "media": {
                                "payload": audio_payload
                            }
                        }
                        await websocket.send_json(audio_delta)


                        if response.get("item_id") and response["item_id"] != last_assistant_item:
                            response_start_timestamp_twilio = latest_media_timestamp
                            last_assistant_item = response["item_id"]
                            if SHOW_TIMING_MATH:
                                print(f"Setting start timestamp for new response: {response_start_timestamp_twilio}ms")

                        await send_mark(websocket, stream_sid)

                    # Emily asked to hang up: answer her tool call, then close the line
                    if response.get('type') == 'response.done':
                        response_in_progress = False
                        note_activity()
                        for out_item in (response.get('response', {}).get('output') or []):
                            if out_item.get('type') == 'function_call' and out_item.get('name') == 'end_call':
                                call_id = out_item.get('call_id')
                                print(f"Emily requested hangup: {call_id}")
                                try:
                                    await openai_ws.send(json.dumps({
                                        "type": "conversation.item.create",
                                        "item": {
                                            "type": "function_call_output",
                                            "call_id": call_id,
                                            "output": json.dumps({"ok": True})
                                        }
                                    }))
                                except Exception:
                                    pass
                                # Let any queued audio finish playing on the line before hanging up
                                await asyncio.sleep(3)
                                try:
                                    await websocket.close()
                                except Exception:
                                    pass
                                return

                    if response.get('type') == 'input_audio_buffer.speech_started':
                        # Caller is speaking: mark it and reset the silence watchdog
                        caller_speaking = True
                        note_activity()

                    # Trigger an interruption only on sustained caller speech, not every
                    # speech_started blip - phone-line noise and echo fire this constantly.
                    if response.get('type') == 'input_audio_buffer.speech_stopped':
                        caller_speaking = False
                        note_activity()
                        audio_end = response.get('audio_end_ms')
                        print(f"Speech stopped detected at {audio_end}ms.")
                        if last_assistant_item:
                            print(f"Interrupting response with id: {last_assistant_item}")
                            await handle_speech_started_event()
            except Exception as e:
                print(f"Error in send_to_twilio: {e}")

        async def handle_speech_started_event():
            """Handle interruption when the caller's speech starts."""
            nonlocal response_start_timestamp_twilio, last_assistant_item
            print("Handling speech started event.")
            if mark_queue and response_start_timestamp_twilio is not None:
                elapsed_time = latest_media_timestamp - response_start_timestamp_twilio
                if SHOW_TIMING_MATH:
                    print(f"Calculating elapsed time for truncation: {latest_media_timestamp} - {response_start_timestamp_twilio} = {elapsed_time}ms")

                if last_assistant_item:
                    if SHOW_TIMING_MATH:
                        print(f"Truncating item with ID: {last_assistant_item}, Truncated at: {elapsed_time}ms")

                    truncate_event = {
                        "type": "conversation.item.truncate",
                        "item_id": last_assistant_item,
                        "content_index": 0,
                        "audio_end_ms": elapsed_time
                    }
                    await openai_ws.send(json.dumps(truncate_event))

                await websocket.send_json({
                    "event": "clear",
                    "streamSid": stream_sid
                })

                mark_queue.clear()
                last_assistant_item = None
                response_start_timestamp_twilio = None

        async def send_mark(connection, stream_sid):
            if stream_sid:
                mark_event = {
                    "event": "mark",
                    "streamSid": stream_sid,
                    "mark": {"name": "responsePart"}
                }
                await connection.send_json(mark_event)
                mark_queue.append('responsePart')

        await asyncio.gather(receive_from_twilio(), send_to_twilio(), inactivity_watchdog())

async def send_initial_conversation_item(openai_ws, call_context_text: str):
    """Send initial conversation item so Emily greets the driver first, with per-call context."""
    initial_conversation_item = {
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": (
                        call_context_text
                        + "\n\nStart the call now: greet the driver with the time-of-day greeting and "
                        "introduce yourself as Emily Smith calling from Sherbet Electric Taxis, London, "
                        "then ask to verify the taxi registration."
                    )
                }
            ]
        }
    }
    await openai_ws.send(json.dumps(initial_conversation_item))
    await openai_ws.send(json.dumps({"type": "response.create"}))


async def initialize_session(openai_ws, call_context_text: str = ""):
    """Control initial session with OpenAI."""
    session_update = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "model": "gpt-realtime",
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": {"type": "audio/pcmu"},
                    "turn_detection": {
                        "type": "server_vad",
                        "threshold": 0.7,
                        "prefix_padding_ms": 300,
                        "silence_duration_ms": 700
                    }
                },
                "output": {
                    "format": {"type": "audio/pcmu"},
                    "voice": VOICE
                }
            },
            "instructions": SYSTEM_MESSAGE,
            "tools": [{
                "type": "function",
                "name": "end_call",
                "description": "Hang up the phone call. Call this ONLY after your full closing sequence: booking confirmation and SMS promise (when a slot was booked), your final goodbye sentence, and the driver's reply or a moment of silence. It disconnects the line instantly.",
                "parameters": {"type": "object", "properties": {}, "required": []}
            }],
            "tool_choice": "auto"
        }
    }
    print('Sending session update:', json.dumps(session_update))
    await openai_ws.send(json.dumps(session_update))

    # Emily speaks first when the call connects
    await send_initial_conversation_item(openai_ws, call_context_text)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
