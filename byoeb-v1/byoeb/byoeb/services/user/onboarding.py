import hashlib
import os
import byoeb.services.user.constants as user_const
import byoeb.services.chat.constants as chat_const
from typing import List
from byoeb.factory import ChannelClientFactory
from byoeb_core.models.byoeb.message_context import (
    ByoebMessageContext,
    MessageContext,
    ReplyContext,
    MessageTypes
)
from byoeb.services.channel.whatsapp import WhatsAppService
from byoeb.services.databases.mongo_db import UserMongoDBService, MessageMongoDBService
from byoeb_core.models.byoeb.user import User
from datetime import datetime, timezone
from byoeb_core.convertor.audio_convertor import wav_to_ogg_opus_bytes
from byoeb_core.models.whatsapp.requests import media_request as wa_media

def get_language_code(language):
    language_dict = {
        "हिंदी": "hi",
        "English": "en",
        "मराठी": "mr",
        "తెలుగు": "te",
    }
    if language in language_dict:
        return language_dict[language]

def get_consent(choice):
    yes = ["हाँ", "Yes", "होय", "అవును"]
    no = ["नहीं", "No" "नाही", "కాదు"]
    if choice in yes:
        return True
    elif choice in no:
        return False

def get_user_type(choice):
    asha = ["Asha", "आशा", "ఆశ"]
    anm = ["ANM", "नर्सदीदी / ए.एन.एम", "నర్స్‌దీది / ఎ.ఎన్.ఎం"]
    if choice in asha:
        return "asha"
    elif choice in anm:
        return "anm"

def _log_reply_context(rc: ReplyContext, where: str):
    try:
        print(
            f"[ReplyContext@{where}] reply_id={rc.reply_id!r}, "
            f"message_category={rc.message_category!r}"
        )
    except Exception as e:
        print(f"[ReplyContext@{where}] <print failed: {e!r}>")

def make_reply_context(from_message: ByoebMessageContext, where: str) -> ReplyContext:
    rc = ReplyContext(
        reply_id=from_message.message_context.message_id,
        message_category=from_message.message_category,
    )
    _log_reply_context(rc, where)
    return rc

def create_user_selection_message(
    message: ByoebMessageContext,
    user_lang: str = None
) -> ByoebMessageContext:
    message_dict = {
        "hi": {
            "text": "🙏🏽 नमस्ते! मैं खुशी बेबी से आशा सहेली हूँ। आप कौन हैं?",
            "options": ["आशा", "नर्सदीदी / ए.एन.एम"]
        },
        "en": {
            "text": "🙏🏽 Namaste! I am ASHA Saheli from Khushi Baby. Who are you?",
            "options": ["Asha", "ANM"]
        },
        "mr": {
            "text": "🙏🏽 नमस्कार! मी खुशी बेबी कडून आशा सहेली आहे. तुम्ही कोण आहात?",
            "options": ["आशा", "नर्सदीदी / ए.एन.एम"]
        },
        "te": {
            "text": "🙏🏽 నమస్తే! నేను ఖుషి బేబీ నుండి ఆశ సహेली. మీరు ఎవరు?",
            "options": ["ఆశ", "నర్స్‌దీది / ఎ.ఎన్.ఎం"]
        }
    }
    text_message = message_dict[user_lang]["text"]
    text_options = message_dict[user_lang]["options"]
    message_type = MessageTypes.INTERACTIVE_BUTTON.value
    button_additional_info = {
        chat_const.BUTTON_TITLES: text_options,
    }
    return ByoebMessageContext(
        channel_type=message.channel_type,
        message_category=user_const.USER_TYPE,
        user=message.user,
        message_context=MessageContext(
            message_type=message_type,
            message_source_text=text_message,
            additional_info=button_additional_info,
        ),
        reply_context=make_reply_context(message, "create_user_selection_message"),
    )

def create_language_selection_message(
    message: ByoebMessageContext
) -> ByoebMessageContext:
    text_message = "अपनी भाषा का चयन करें।\nतुमची भाषा निवडा\nSelect your language\nమీ భాషను ఎంచుకోండి"
    lang_list = ["हिंदी", "मराठी", "English", "తెలుగు"]
    interactive_list_additional_info = {
        chat_const.DESCRIPTION: "भाषा चुनें:",
        chat_const.ROW_TEXTS: lang_list,
    }
    message_type = MessageTypes.INTERACTIVE_LIST.value
    return ByoebMessageContext(
        channel_type=message.channel_type,
        message_category=user_const.LANGUAGE_SELECTION,
        user=message.user,
        message_context=MessageContext(
            message_type=message_type,
            message_source_text=text_message,
            additional_info=interactive_list_additional_info,
        ),
        reply_context=make_reply_context(message, "create_language_selection_message"),
    )

def create_consent_message(
    message: ByoebMessageContext,
    user_type: str = None
) -> ByoebMessageContext:
    consent_dict = {
        "asha": {
            "hi": {
                "text": """मैं आशा सहेली हूँ, खुशी बेबी द्वारा निःशुल्क प्रदान किया गया 24x7 टूल। मैं आपके आशा कार्य से जुड़े किसी भी प्रश्न का उत्तर देने के लिए यहाँ हूँ।\nशोधकर्ता केवल शोध उद्देश्यों के लिए आपके संदेशों को रिकॉर्ड और विश्लेषण करेंगे, और इन्हें किसी एएनएम, सीएचओ या सरकारी अधिकारी के साथ साझा नहीं किया जाएगा। यदि आप सहमत हैं, तो कृपया 'हाँ' पर क्लिक करें या मुझे संदेश भेजना जारी रखें; अन्यथा, 'नहीं' कहें।""",
                "options": ["हाँ", "नहीं"]
            },
            "en": {
                "text": """I am ASHA Saheli, a free-of-charge, 24x7 tool offered by Khushi Baby. I am here to answer any questions you have about your work as an ASHA.\nResearchers will log and analyze your messages only for research purposes, and won't share it with any ANM, CHO, or government official. If you agree, please click 'Yes' or continue to send me messages; otherwise, say 'No.'""",
                "options": ["Yes", "No"]
            },
            "mr": {
                "text": """मी आशा सहेली आहे, खुशी बेबी कडून मोफत उपलब्ध असलेले 24x7 टूल. मी तुमच्या आशा कार्याशी संबंधित कोणत्याही प्रश्नाचे उत्तर देण्यासाठी येथे आहे.\nशोधक फक्त संशोधन उद्देशांसाठी तुमच्या संदेशांचे रेकॉर्ड आणि विश्लेषण करतील, आणि ते कोणत्याही एएनएम, सीएचओ किंवा सरकारी अधिकाऱ्यांबरोबर सामायिक करणार नाहीत. तुम्ही सहमत असाल तर कृपया 'होय' वर क्लिक करा किंवा मला संदेश पाठवणे सुरू ठेवा; अन्यथा, 'नाही' सांगा.""",
                "options": ["होय", "नाही"]
            },
            "te": {
                "text": """నేను ఆశ సహेली, ఖుషి బేబీ అందించిన ఉచిత 24x7 టూల్. నేను మీ ఆశా పనికి సంబంధించిన ఏదైనా ప్రశ్నకు సమాధానం ఇవ్వడానికి ఇక్కడ ఉన్నాను.\nశోధకులు మీ సందేశాలను పరిశోధన ప్రయోజనాల కోసం మాత్రమే నమోదు చేసి విశ్లేషిస్తారు మరియు వాటిని ఏ ఎఎన్‌ఎం, సిహెచ్‌ఓ లేదా ప్రభుత్వ అధికారితో పంచుకోరు. మీరు అంగీకరిస్తే, దయచేసి 'అవును' క్లిక్ చేయండి లేదా నాకు సందేశాలు పంపడం కొనసాగించండి; లేదంటే, 'కాదు' అని చెప్పండి.""",
                "options": ["అవును", "కాదు"]
            }
        },
        "anm": {
            "hi": {
                "text": """मैं आशा सहेली हूँ, खुशी बेबी द्वारा निःशुल्क प्रदान किया गया 24x7 टूल। मैं आशाओं के कार्य से जुड़े किसी भी प्रश्न का उत्तर देने के लिए यहाँ हूँ। यदि मुझे किसी आशा के प्रश्न का उत्तर नहीं पता होता, तो मैं आपसे सहायता मांगूँगी। \nशोधकर्ता केवल शोध उद्देश्यों के लिए आपके संदेशों को रिकॉर्ड और विश्लेषण करेंगे, और इन्हें किसी आशा, सीएचओ या सरकारी अधिकारी के साथ साझा नहीं किया जाएगा। यदि आप सहमत हैं, तो कृपया 'हाँ' पर क्लिक करें या मुझे संदेश भेजना जारी रखें; अन्यथा, 'नहीं' कहें।""",
                "options": ["हाँ", "नहीं"]
            },
            "en": {
                "text": """I am ASHA Saheli, a free-of-charge, 24x7 tool offered by Khushi Baby. I am here to answer any questions ASHAs have about their work. Whenever I do not know the answer to an ASHA's question, I will request you for help.\nResearchers will log and analyze your messages only for research purposes, and won't share it with any ASHA, CHO, or government official. If you agree, please click 'Yes' or continue to send me messages; otherwise, say 'No.'""",
                "options": ["Yes", "No"]
            },
            "mr": {
                "text": """मी आशा सहेली आहे, खुशी बेबी कडून मोफत उपलब्ध असलेला 24x7 टूल. मी आशांच्या कार्याशी संबंधित कोणत्याही प्रश्नाचे उत्तर देण्यासाठी येथे आहे. जेव्हा मला एखाद्या आशा च्या प्रश्नाचे उत्तर माहित नसते, तेव्हा मी तुमच्याकडून मदतीची विनंती करीन.\nशोधक फक्त संशोधन उद्देशांसाठी तुमच्या संदेशांचे रेकॉर्ड आणि विश्लेषण करतील, आणि ते कोणत्याही आशा, सीएचओ किंवा सरकारी अधिकाऱ्यांबरोबर सामायिक करणार नाहीत. तुम्ही सहमत असाल तर कृपया 'होय' वर क्लिक करा किंवा मला संदेश पाठवणे सुरू ठेवा; अन्यथा, 'नाही' सांगा.""",
                "options": ["होय", "नाही"]
            },
            "te": {
                "text": """నేను ఆశ సహेली, ఖుషి బేబీ అందించిన ఉచిత 24x7 టూల్. నేను ఆశలకు సంబంధించిన ఏదైనా ప్రశ్నకు సమాధానం ఇవ్వడానికి ఇక్కడ ఉన్నాను. ASHA ప్రశ్నకు నాకు సమాధానం తెలియకపోతే, నేను మీరందరినీ సహాయం కోరుతాను.\nశోధకులు మీ సందేశాలను పరిశోధన ప్రయోజనాల కోసం మాత్రమే నమోదు చేసి విశ్లేషిస్తారు మరియు వాటిని ఏ ASHA, CHO లేదా ప్రభుత్వ అధికారితో పంచుకోరు. మీరు అంగీకరిస్తే, దయచేసి 'అవును' క్లిక్ చేయండి లేదా నాకు సందేశాలు పంపడం కొనసాగించండి; లేదంటే, 'కాదు' అని చెప్పండి.""",
                "options": ["అవును", "కాదు"]
            }
        }  
    }
    text_message = consent_dict[user_type][message.user.user_language]["text"]
    text_options = consent_dict[user_type][message.user.user_language]["options"]
    message_type = MessageTypes.INTERACTIVE_BUTTON.value
    button_additional_info = {
        chat_const.BUTTON_TITLES: text_options,
    }
    return ByoebMessageContext(
        channel_type=message.channel_type,
        message_category=user_const.CONSENT,
        user=message.user,
        message_context=MessageContext(
            message_type=message_type,
            message_source_text=text_message,
            additional_info=button_additional_info,
        ),
        reply_context=make_reply_context(message, "create_consent_message"),
    )

def create_audio(
    user_lang: str,
    user_type: str
):
    # Get the directory of the current script
    current_dir = os.path.dirname(os.path.abspath(__file__))
    audio_path = os.path.join(current_dir, 'onboarding', user_lang, f'welcome_messages_{user_type}.wav')
    audio_path = os.path.normpath(audio_path)
    audio_bytes = None
    with open(audio_path, 'rb') as file:
        audio_bytes = file.read()
    ogg_bytes = wav_to_ogg_opus_bytes(audio_bytes)
    media_type=wa_media.FileMediaType.AUDIO_OGG.value
    return ogg_bytes, media_type
    
        
def create_initial_message(
    message: ByoebMessageContext
) -> ByoebMessageContext:
    user_type = message.user.user_type
    user_lang = message.user.user_language
    thank_you_dict = {
        "asha": {
            "hi": "आप मुझसे गर्भावस्था, शिशु देखभाल और सामान्य स्वास्थ्य से जुड़े किसी भी प्रश्न को टाइप करके या वॉयस मैसेज 🎙️ भेजकर पूछ सकते हैं। \nआपकी बातचीत मुझसे गोपनीय रहेगी। यदि मुझे आपके किसी प्रश्न का उत्तर नहीं पता होगा, तो मैं इसे एएनएम को भेजूंगी। हालांकि, एएनएम को यह नहीं पता चलेगा कि प्रश्न किस आशा ने पूछा है।\nइसलिए, बिना किसी झिझक के मुझसे अपने सभी प्रश्न पूछें।",
            "en": "You can ask me any question about pregnancy, childcare, and health in general, by typing or sending me a voice message 🎙️.\nYour conversation with me is private. If I don't know the answer to a question you ask me, I will send it to an ANM. However, the ANM won't know the identity of the ASHA who asked the question. Feel free to ask any questions to me without hesitation.",
            "mr": "तुम्ही गर्भावस्था, शिशु देखभाल आणि आरोग्याबद्दल कोणतेही प्रश्न मला टाइप करून किंवा वॉयस मेसेज 🎙️ पाठवून विचारू शकता.\nतुमची माझ्याशी गोपनीयता राहील. जर मला तुमच्या विचारलेल्या प्रश्नाचे उत्तर माहित नसेल तर मी ते एएनएम कडे पाठवीन. तथापि, एएनएमला प्रश्न कोणत्या आशाने विचारला हे माहित होणार नाही.\nम्हणजेच, तुम्ही कोणतीही शंका मनाशी ठेऊ नका आणि मला विचारा.",
            "te": "మీరు గర్భధారణ, శిశు సంరక్షణ మరియు ఆరోగ్యం గురించి ఏదైనా ప్రశ్నను టైప్ చేయడం లేదా వాయిస్ సందేశం 🎙️ పంపడం ద్వారా అడగవచ్చు.\nమీతో నా సంభాషణ గోప్యంగా ఉంటుంది. మీరు అడిగే ప్రశ్నకు నాకు సమాధానం తెలియకపోతే, నేను దానిని ANM కు పంపుతాను. అయితే, ANM ఎవరు ASHA ప్రశ్న అడిగిందో తెలియదు. కాబట్టి, ఎటువంటి సంకోచం లేకుండా నాకు ఏవైనా ప్రశ్నలు అడగండి."
        },
        "anm": {
            "hi": "यदि मुझे किसी आशा के प्रश्न का उत्तर नहीं पता होगा, तो मैं आपसे सहायता मांगूंगी। आप अपने उत्तर टाइप करके या वॉयस मैसेज 🎙️ भेजकर दे सकते हैं।\nआपकी बातचीत मुझसे गोपनीय रहेगी। बिना किसी झिझक के अपने उत्तर साझा करें।",
            "en": "Whenever I do not know the answer to an ASHA's question, I will request you for help. You can answer the questions by typing or sending me a voice message 🎙️.\nYour conversation with me is private. Feel free to share any answers without hesitation.",
            "mr": "जेव्हा मला एखाद्या आशा च्या प्रश्नाचे उत्तर माहित नसते, तेव्हा मी तुमच्याकडून मदतीची विनंती करीन. तुम्ही प्रश्नांचे उत्तर टाइप करून किंवा वॉयस मेसेज 🎙️ पाठवून देऊ शकता.\nतुमची माझ्याशी गोपनीयता राहील. तुम्ही कोणतीही शंका मनाशी ठेऊ नका आणि तुमचे उत्तर द्या.",
            "te": "ASHA ప్రశ్నకు నాకు సమాధానం తెలియకపోతే, నేను మీరందరినీ సహాయం కోరుతాను. మీరు ప్రశ్నలకు సమాధానం టైప్ చేయడం లేదా వాయిస్ సందేశం 🎙️ పంపడం ద్వారా ఇవ్వవచ్చు.\nమీతో నా సంభాషణ గోప్యంగా ఉంటుంది. ఎటువంటి సంకోచం లేకుండా మీ సమాధానాలను పంచుకోండి."
        }
    }
    related_questions = {
        "description": {
            "en": "Suggested Questions",
            "hi": "सुझाए गए प्रश्न",
            "mr": "सूचवलेले प्रश्न",
            "te": "సూచించిన ప్రశ్నలు"
        },
        "questions": {
            "en": [
                "How much does a 1-year-old typically weigh?",
                "What long-term effects does tobacco cause?",
                "What is Antara injection?"
            ],
            "hi": [
                "1 साल का बच्चा आमतौर पर कितना वज़न रखता है?",
                "तंबाकू के दीर्घकालिक प्रभाव क्या होते हैं?",
                "अंतरा इंजेक्शन क्या है?"
            ],
            "mr": [
                "1 वर्षाचा मुलगा सामान्यतः किती वजनाचा असतो?",
                "तंबाकूचे दीर्घकालीन परिणाम काय आहेत?",
                "अंतरा इंजेक्शन म्हणजे काय?"
            ],
            "te": [
                "ఒక సంవత్సరానికి చెందిన బిడ్డ సాధారణంగా ఎంత బరువు ఉంటుంది?",
                "తంబాకుకు దీర్ఘకాలిక ప్రభావాలు ఏమిటి?",
                "అంతర ఇంజెక్షన్ అంటే ఏమిటి?"
            ]
        }
    }
    audio_bytes, audio_type = create_audio(user_lang, user_type)
    text_message = thank_you_dict[user_type][user_lang]
    if user_type == "anm":
        message_type = MessageTypes.REGULAR_TEXT.value
        return ByoebMessageContext(
            channel_type=message.channel_type,
            message_category=user_const.THANK_YOU,
            user=message.user,
            message_context=MessageContext(
                message_type=message_type,
                message_source_text=text_message,
                additional_info = {
                    chat_const.DATA: audio_bytes,
                    chat_const.MIME_TYPE: audio_type,
                }
            ),
            reply_context=make_reply_context(message, "create_initial_message[ANM]"),
        )
    return ByoebMessageContext(
        channel_type=message.channel_type,
        message_category=user_const.THANK_YOU,
        user=message.user,
        message_context=MessageContext(
            message_type=MessageTypes.INTERACTIVE_LIST.value,
            message_source_text=text_message,
            additional_info = {
                chat_const.DESCRIPTION: related_questions["description"][user_lang],
                chat_const.ROW_TEXTS: related_questions["questions"][user_lang],
                chat_const.DATA: audio_bytes,
                chat_const.MIME_TYPE: audio_type,
            }
        ),
        reply_context=make_reply_context(message, "create_initial_message[non-ANM]"),
    )

def create_user(
    phone_number_id: str,
    language: str = None,
    user_type: str = None,
    consent: bool = None,
) -> User:
    return User(
        user_id=hashlib.md5(phone_number_id.encode()).hexdigest(),
        phone_number_id=phone_number_id,
        user_language=language,
        user_type=user_type,
        additional_info={
            user_const.CONSENT: consent,
        },
        test_user=(user_type == "others"),
        experts={},
        audience=[],
        created_timestamp=int(datetime.now(timezone.utc).timestamp()),
        activity_timestamp=int(datetime.now(timezone.utc).timestamp()),
    )
    
async def handle_unknown_user(
    messages: List[ByoebMessageContext],
    message_db_service: MessageMongoDBService,
    user_db_service: UserMongoDBService,
    channel_factory: ChannelClientFactory,
):
    print("handle_unknown_user")
    channel_service = WhatsAppService(channel_client_factory=channel_factory)
    if not isinstance(channel_service, WhatsAppService):
        raise ValueError("Invalid channel service type")
    for message in messages:
        print("message.reply_context", message.reply_context)
        if message.reply_context is None or message.reply_context.reply_id is None:
            print(f"onboarding message: {message}")
            byoeb_message = create_language_selection_message(message)
            requests = channel_service.prepare_requests(byoeb_message)
            responses, message_ids = await channel_service.send_requests(requests)
            convs = channel_service.create_conv(byoeb_message, responses)
            new_user = create_user(phone_number_id=message.user.phone_number_id)
            print(new_user)
            message_db_queries = {
                chat_const.CREATE: message_db_service.message_create_queries(convs)
            }
            user_db_queries = {
                chat_const.CREATE: [user_db_service.user_create_query(new_user)]
            }
            try:
                await message_db_service.execute_queries(message_db_queries)
                await user_db_service.execute_queries(user_db_queries)
            except Exception as e:
                print(f"Error in onboarding message: {e}")
        elif message.reply_context.message_category == chat_const.LANGUAGE_SELECTION:
            print("Language Selection")
            text = message.message_context.message_source_text
            code = get_language_code(text)
            update_user = create_user(
                phone_number_id=message.user.phone_number_id,
                language=code,
            )
            user_db_queries = {
                chat_const.UPDATE: [user_db_service.user_update_query(update_user)]
            }
            byoeb_message = create_user_selection_message(message, code)
            requests = channel_service.prepare_requests(byoeb_message)
            responses, message_ids = await channel_service.send_requests(requests)
            convs = channel_service.create_conv(byoeb_message, responses)
            message_db_queries = {
                chat_const.CREATE: message_db_service.message_create_queries(convs)
            }
            await message_db_service.execute_queries(message_db_queries)
            await user_db_service.execute_queries(user_db_queries)
        elif message.reply_context.message_category == chat_const.USER_TYPE:
            print("User Type")
            text = message.message_context.message_source_text
            user_type = get_user_type(text)
            update_user = create_user(
                phone_number_id=message.user.phone_number_id,
                language=message.user.user_language,
                user_type=user_type,
            )
            user_db_queries = {
                chat_const.UPDATE: [user_db_service.user_update_query(update_user)]
            }
            byoeb_message = create_consent_message(message, user_type)
            requests = channel_service.prepare_requests(byoeb_message)
            responses, message_ids = await channel_service.send_requests(requests)
            convs = channel_service.create_conv(byoeb_message, responses)
            message_db_queries = {
                chat_const.CREATE: message_db_service.message_create_queries(convs)
            }
            await message_db_service.execute_queries(message_db_queries)
            await user_db_service.execute_queries(user_db_queries)
        elif message.reply_context.message_category == chat_const.CONSENT:
            print("Consent")
            text = message.message_context.message_source_text
            consent = get_consent(text)
            print(f"consent: {consent}")
            update_user = create_user(
                phone_number_id=message.user.phone_number_id,
                user_type=message.user.user_type,
                language=message.user.user_language,
                consent=consent
            )
            user_db_queries = {
                chat_const.UPDATE: [user_db_service.user_update_query(update_user)]
            }
            byoeb_message = create_initial_message(message)
            byoeb_message_no_reply = byoeb_message.model_copy(deep=True)
            byoeb_message_no_reply.reply_context = None
            # print(f"Initial message: {byoeb_message}")
            requests = channel_service.prepare_requests(byoeb_message_no_reply)
            responses, message_ids = await channel_service.send_requests(requests)
            await user_db_service.execute_queries(user_db_queries)
        else:
            print(f"onboarding message: {message}")
            byoeb_message = create_language_selection_message(message)
            requests = channel_service.prepare_requests(byoeb_message)
            responses, message_ids = await channel_service.send_requests(requests)
            convs = channel_service.create_conv(byoeb_message, responses)
            new_user = create_user(phone_number_id=message.user.phone_number_id)
            message_db_queries = {
                chat_const.CREATE: message_db_service.message_create_queries(convs)
            }
            user_db_queries = {
                chat_const.CREATE: [user_db_service.user_create_query(new_user)]
            }
            try:
                await message_db_service.execute_queries(message_db_queries)
                await user_db_service.execute_queries(user_db_queries)
            except Exception as e:
                print(f"Error in onboarding message: {e}")