from .user_enums import UserType, LanguageCode

# Display names shown in the language selector list
LANGUAGE_DISPLAY_NAMES = ["हिंदी", "मराठी", "English", "తెలుగు"]

# Map display name -> LanguageCode.value for get_language_code()
LANGUAGE_NAME_TO_CODE = {
    "हिंदी": LanguageCode.HINDI.value,
    "English": LanguageCode.ENGLISH.value,
    "मराठी": LanguageCode.MARATHI.value,
    "తెలుగు": LanguageCode.TELUGU.value,
}

# Message shown to ask "Who are you?"
MESSAGE_DICT = {
    LanguageCode.HINDI.value: {
        "text": "🙏🏽 नमस्ते! मैं खुशी बेबी से आशा सहेली हूँ। आप कौन हैं?",
        "options": ["आशा", "नर्सदीदी / ए.एन.एम", "अन्य"],
    },
    LanguageCode.ENGLISH.value: {
        "text": "🙏🏽 Namaste! I am ASHA Saheli from Khushi Baby. Who are you?",
        "options": ["Asha", "ANM", "Others"],
    },
    LanguageCode.MARATHI.value: {
        "text": "🙏🏽 नमस्कार! मी खुशी बेबी कडून आशा सहेली आहे. तुम्ही कोण आहात?",
        "options": ["आशा", "नर्सदीदी / ए.एन.एम", "इतर"],
    },
    LanguageCode.TELUGU.value: {
        "text": "🙏🏽 నమస్తే! నేను ఖుషి బేబీ నుండి ఆశ సహेली. మీరు ఎవరు?",
        "options": ["ఆశ", "నర్స్‌దీది / ఎ.ఎన్.ఎం", "ఇతరులు"],
    },
}

# Consent content per user type
CONSENT_DICT = {
    UserType.ASHA.value: {
        LanguageCode.HINDI.value: {
            "text": """मैं आशा सहेली हूँ, खुशी बेबी द्वारा निःशुल्क प्रदान किया गया 24x7 टूल। मैं आपके आशा कार्य से जुड़े किसी भी प्रश्न का उत्तर देने के लिए यहाँ हूँ।
शोधकर्ता केवल शोध उद्देश्यों के लिए आपके संदेशों को रिकॉर्ड और विश्लेषण करेंगे, और इन्हें किसी एएनएम, सीएचओ या सरकारी अधिकारी के साथ साझा नहीं किया जाएगा। यदि आप सहमत हैं, तो कृपया 'हाँ' पर क्लिक करें या मुझे संदेश भेजना जारी रखें; अन्यथा, 'नहीं' कहें.""",
            "options": ["हाँ", "नहीं"],
        },
        LanguageCode.ENGLISH.value: {
            "text": """I am ASHA Saheli, a free-of-charge, 24x7 tool offered by Khushi Baby. I am here to answer any questions you have about your work as an ASHA.
Researchers will log and analyze your messages only for research purposes, and won't share it with any ANM, CHO, or government official. If you agree, please click 'Yes' or continue to send me messages; otherwise, say 'No.'""",
            "options": ["Yes", "No"],
        },
        LanguageCode.MARATHI.value: {
            "text": """मी आशा सहेली आहे, खुशी बेबी कडून मोफत उपलब्ध असलेले 24x7 टूल. मी तुमच्या आशा कार्याशी संबंधित कोणत्याही प्रश्नाचे उत्तर देण्यासाठी येथे आहे.
शोधक फक्त संशोधन उद्देशांसाठी तुमच्या संदेशांचे रेकॉर्ड आणि विश्लेषण करतील, आणि ते कोणत्याही एएनएम, सीएचओ किंवा सरकारी अधिकाऱ्यांबरोबर सामायिक करणार नाहीत. तुम्ही सहमत असाल तर कृपया 'होय' वर क्लिक करा किंवा मला संदेश पाठवणे सुरू ठेवा; अन्यथा, 'नाही' सांगा.""",
            "options": ["होय", "नाही"],
        },
        LanguageCode.TELUGU.value: {
            "text": """నేను ఆశ సహेली, ఖుషి బేబీ అందించిన ఉచిత 24x7 టూల్. నేను మీ ఆశా పనికి సంబంధించిన ఏదైనా ప్రశ్నకు సమాధానం ఇవ్వడానికి ఇక్కడ ఉన్నాను.
శోధకులు మీ సందేశాలను పరిశోధన ప్రయోజనాల కోసం మాత్రమే నమోదు చేసి విశ్లేషిస్తారు మరియు వాటిని ఏ ఎఎన్‌ఎం, సిహెచ్‌ఓ లేదా ప్రభుత్వ అధికారితో పంచుకోరు. మీరు అంగీకరిస్తే, దయచేసి 'అవును' క్లిక్ చేయండి లేదా నాకు సందేశాలు పంపడం కొనసాగించండి; లేదంటే, 'కాదు' అని చెప్పండి.""",
            "options": ["అవును", "కాదు"],
        },
    },
    UserType.ANM.value: {
        LanguageCode.HINDI.value: {
            "text": """मैं आशा सहेली हूँ, खुशी बेबी द्वारा निःशुल्क प्रदान किया गया 24x7 टूल। मैं आशाओं के कार्य से जुड़े किसी भी प्रश्न का उत्तर देने के लिए यहाँ हूँ। यदि मुझे किसी आशा के प्रश्न का उत्तर नहीं पता होता, तो मैं आपसे सहायता मांगूँगी।
शोधकर्ता केवल शोध उद्देश्यों के लिए आपके संदेशों को रिकॉर्ड और विश्लेषण करेंगे, और इन्हें किसी आशा, सीएचओ या सरकारी अधिकारी के साथ साझा नहीं किया जाएगा। यदि आप सहमत हैं, तो कृपया 'हाँ' पर क्लिक करें या मुझे संदेश भेजना जारी रखें; अन्यथा, 'नहीं' कहें.""",
            "options": ["हाँ", "नहीं"],
        },
        LanguageCode.ENGLISH.value: {
            "text": """I am ASHA Saheli, a free-of-charge, 24x7 tool offered by Khushi Baby. I am here to answer any questions ASHAs have about their work. Whenever I do not know the answer to an ASHA's question, I will request you for help.
Researchers will log and analyze your messages only for research purposes, and won't share it with any ASHA, CHO, or government official. If you agree, please click 'Yes' or continue to send me messages; otherwise, say 'No.'""",
            "options": ["Yes", "No"],
        },
        LanguageCode.MARATHI.value: {
            "text": """मी आशा सहेली आहे, खुशी बेबी कडून मोफत उपलब्ध असलेला 24x7 टूल. मी आशांच्या कार्याशी संबंधित कोणत्याही प्रश्नाचे उत्तर देण्यासाठी येथे आहे. जेव्हा मला एखाद्या आशा च्या प्रश्नाचे उत्तर माहित नसते, तेव्हा मी तुमच्याकडून मदतीची विनंती करीन.
शोधक फक्त संशोधन उद्देशांसाठी तुमच्या संदेशांचे रेकॉर्ड आणि विश्लेषण करतील, आणि ते कोणत्याही आशा, सीएचओ किंवा सरकारी अधिकाऱ्यांबरोबर सामायिक करणार नाहीत. तुम्ही सहमत असाल तर कृपया 'होय' वर क्लिक करा किंवा मला संदेश पाठवणे सुरू ठेवा; अन्यथा, 'नाही' सांगा.""",
            "options": ["होय", "नाही"],
        },
        LanguageCode.TELUGU.value: {
            "text": """నేను ఆశ సహेली, ఖుషి బేబీ అందించిన ఉచిత 24x7 టూల్. నేను ఆశలకు సంబంధించిన ఏదైనా ప్రశ్నకు సమాధానం ఇవ్వడానికి ఇక్కడ ఉన్నాను. ASHA ప్రశ్నకు నాకు సమాధానం తెలియకపోతే, నేను మీరందరినీ సహాయం కోరుతాను.
శోధకులు మీ సందేశాలను పరిశోధన ప్రయోజనాల కోసం మాత్రమే నమోదు చేసి విశ్లేషిస్తారు మరియు వాటిని ఏ ASHA, CHO లేదా ప్రభుత్వ అధికారితో పంచుకోరు. మీరు అంగీకరిస్తే, దయచేసి 'అవును' క్లిక్ చేయండి లేదా నాకు సందేశాలు పంపడం కొనసాగించండి; లేదంటే, 'కాదు' అని చెప్పండి.""",
            "options": ["అవును", "కాదు"],
        },
    },
}

# Message for already onboarded users
ALREADY_REGISTERED_DICT = {
    LanguageCode.HINDI.value: "आप पहले से ही सिस्टम में पंजीकृत हैं।",
    LanguageCode.ENGLISH.value: "You are already registered with the system.",
    LanguageCode.MARATHI.value: "तुम्ही आधीच सिस्टममध्ये नोंदणीकृत आहात.",
    LanguageCode.TELUGU.value: "మీరు ఇప్పటికే సిస్టమ్‌లో నమోదు చేయబడ్డారు.",
}

THANK_YOU_DICT = {
    UserType.ASHA.value: {
        LanguageCode.HINDI.value: "आप मुझसे गर्भावस्था, शिशु देखभाल और आशा के रूप में अपने काम के बारे में कोई भी प्रश्न 💬 लिख कर या🎙️वॉइस संदेश भेजकर पूछ सकते हैं। जैसे की:\nछाया टैबलेट कब लें?\nआभा आईडी क्या है?\n3 महीने के बच्चे को कौन से टीके दें?",
        LanguageCode.ENGLISH.value: "You can ask me any question about pregnancy, childcare, and your work as an ASHA, by typing 💬 or sending me a voice message 🎙️. Like this:\nWhen to take chhaya tablet?\nWhat is ABHA ID?\nWhat vaccines to give a 3-month-old?",
        LanguageCode.MARATHI.value: "तुम्ही गर्भावस्था, शिशु देखभाल आणि आशा म्हणून तुमच्या कामाबद्दल कोणताही प्रश्न 💬 टाइप करून किंवा 🎙️ व्हॉइस संदेश पाठवून विचारू शकता. उदाहरणार्थ:\nछाया टॅबलेट कधी घ्यावे?\nआभा आयडी म्हणजे काय?\n3 महिन्याच्या बाळाला कोणते लसीकरण द्यावे?",
        LanguageCode.TELUGU.value: "మీరు గర్భధారణ, శిశు సంరక్షణ మరియు ఆశగా మీ పని గురించి 💬 టైప్ చేసి లేదా 🎙️ వాయిస్ సందేశం పంపడం ద్వారా నన్ను ఏదైనా ప్రశ్న అడగవచ్చు. ఇలా:\nఛాయా టాబ్లెట్ ఎప్పుడు తీసుకోవాలి?\nఆభా ఐడి అంటే ఏమిటి?\n3 నెలల బిడ్డకు ఏ వ్యాక్సిన్లు ఇవ్వాలి?",
    },
    UserType.ANM.value: {
        LanguageCode.HINDI.value: "इस कार्यक्रम के बारे में अधिक जानकारी के लिए कृपया हमारे हेल्पडेस्क नंबर +91 77270 79678 पर कॉल करें। आपके समर्थन के लिए धन्यवाद 😊",
        LanguageCode.ENGLISH.value: "For more information about this program, please call our helpdesk number +91 77270 79678. Thank you for your support 😊",
        LanguageCode.MARATHI.value: "या कार्यक्रमाबद्दल अधिक माहितीसाठी कृपया आमच्या हेल्पडेस्क नंबर +91 77270 79678 वर कॉल करा. तुमच्या समर्थनाबद्दल धन्यवाद 😊",
        LanguageCode.TELUGU.value: "ఈ ప్రోగ్రాం గురించి మరిన్ని సమాచారం కోసం దయచేసి మా హెల్ప్‌డెస్క్ నంబర్ +91 77270 79678 కి కాల్ చేయండి. మీ మద్దతుకు ధన్యవాదాలు 😊",
    },
    # Note: OTHERS user type uses ASHA messages (fallback handled in code)
}

# Common onboarding phrases accepted in all languages (ASHA, ANM, Others variants).
# Used together with language-specific phrases to avoid duplication.
ONBOARD_GLOBAL_PHRASES = [
    "onboard asha",
    "onboard-asha",
    "onboard anm",
    "onboard-anm",
    "onboard others",
    "onboard-others",
]

# Language-specific onboarding phrases only (no English "onboard *" here; those are in ONBOARD_GLOBAL_PHRASES).
ONBOARD_LANGUAGE_SPECIFIC_PHRASES = {
    LanguageCode.HINDI.value: [
        "में एक आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है",
        "मैं आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है",
        "आशा सहेली बोट से जुड़ना है",
        "आशा सहेली से जुड़ना है",
    ],
    LanguageCode.ENGLISH.value: [
        "ONBOARD ASHA",
        "ONBOARD ANM",
        "ONBOARD OTHERS",
    ],
    LanguageCode.MARATHI.value: [
        "मी आशा आहे आणि मला आशा सहेली बॉटमध्ये सामील व्हायचे आहे",
        "आशा सहेली बॉटमध्ये सामील व्हायचे आहे",
        "आशा सहेली बॉटमध्ये सामील",
        "आशा सहेली सामील व्हायचे आहे",
    ],
    LanguageCode.TELUGU.value: [
        "నేను ఆశాను మరియు ఆశా సహేలి బాట్‌లో చేరాలనుకుంటున్నాను",
        "ఆశా సహేలి బాట్‌లో చేరాలనుకుంటున్నాను",
        "ఆశా సహేలి బాట్‌లో చేరాలి",
        "ఆశా సహేలి చేరాలనుకుంటున్నాను",
    ],
}

# Phrases that indicate user wants to register (onboarding intent). Used by is_onboard() and the
# language-selection guard. Each language: language-specific phrases + global phrases.
ONBOARD_WELCOME_MESSAGE_DICT = {
    lang: ONBOARD_LANGUAGE_SPECIFIC_PHRASES[lang] + ONBOARD_GLOBAL_PHRASES
    for lang in ONBOARD_LANGUAGE_SPECIFIC_PHRASES
}

# Shown when user is in onboarding path but message is not onboarding-like. Role is not yet known.
REGISTER_PROMPT_TEXT = (
    "To register with ASHA Saheli, please send 'onboard asha', 'onboard anm', or 'onboard others'."
)

# Suggested questions list text + items
RELATED_QUESTIONS = {
    "description": {
        LanguageCode.ENGLISH.value: "Suggested Questions",
        LanguageCode.HINDI.value: "सुझाए गए प्रश्न",
        LanguageCode.MARATHI.value: "सूचवलेले प्रश्न",
        LanguageCode.TELUGU.value: "సూచించిన ప్రశ్నలు",
    },
    "questions": {
        LanguageCode.ENGLISH.value: [
            "How much does a 1-year-old typically weigh?",
            "What long-term effects does tobacco cause?",
            "What is Antara injection?",
        ],
        LanguageCode.HINDI.value: [
            "1 साल का बच्चा आमतौर पर कितना वज़न रखता है?",
            "तंबाकू के दीर्घकालिक प्रभाव क्या होते हैं?",
            "अंतरा इंजेक्शन क्या है?",
        ],
        LanguageCode.MARATHI.value: [
            "1 वर्षाचा मुलगा सामान्यतः किती वजनाचा असतो?",
            "तंबाकूचे दीर्घकालीन परिणाम काय आहेत?",
            "अंतरा इंजेक्शन म्हणजे काय?",
        ],
        LanguageCode.TELUGU.value: [
            "ఒక సంవత్సరానికి చెందిన బిడ్డ సాధారణంగా ఎంత బరువు ఉంటుంది?",
            "తంబాకుకు దీర్ఘకాలిక ప్రభావాలు ఏమిటి?",
            "అంతర ఇంజెక్షన్ అంటే ఏమిటి?",
        ],
    },
}

# Button text matchers for consent
YES_SET = {"हाँ", "Yes", "होय", "అవును"}
NO_SET  = {"नहीं", "No", "नाही", "కాదు"}

# Strings users click for user-type selection, grouped by canonical user type
USER_TYPE_OPTIONS = {
    UserType.ASHA.value:    {"Asha", "आशा", "ఆశ"},
    UserType.ANM.value:     {"ANM", "नर्सदीदी / ए.एन.एम", "నర్స్‌దీది / ఎ.ఎన్.ఎం"},
    UserType.OTHERS.value:  {"Others", "अन्य", "इतर", "ఇతరులు"},
}