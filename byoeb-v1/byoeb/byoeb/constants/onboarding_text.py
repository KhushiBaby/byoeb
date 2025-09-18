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

THANK_YOU_DICT = {
    UserType.ASHA.value: {
        LanguageCode.HINDI.value: "आप मुझसे गर्भावस्था, शिशु देखभाल ... (truncated for brevity in this file)",
        LanguageCode.ENGLISH.value: "You can ask me any question about pregnancy, childcare, ...",
        LanguageCode.MARATHI.value: "तुम्ही गर्भावस्था, शिशु देखभाल ...",
        LanguageCode.TELUGU.value: "మీరు గర్భధారణ, శిశు సంరక్షణ ...",
    },
    UserType.ANM.value: {
        LanguageCode.HINDI.value: "यदि मुझे किसी आशा के प्रश्न का उत्तर नहीं पता होगा, ...",
        LanguageCode.ENGLISH.value: "Whenever I do not know the answer to an ASHA's question, ...",
        LanguageCode.MARATHI.value: "जेव्हा मला एखाद्या आशा च्या प्रश्नाचे उत्तर माहित नसते, ...",
        LanguageCode.TELUGU.value: "ASHA ప్రశ్నకు నాకు సమాధానం తెలియకపోతే, ...",
    },
}

ONBOARD_WELCOME_MESSAGE_DICT = {
    LanguageCode.HINDI.value: ["में एक आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है"],
    LanguageCode.ENGLISH.value: ["onboard asha"],
    LanguageCode.MARATHI.value: ["मी आशा आहे आणि मला आशा सहेली बॉटमध्ये सामील व्हायचे आहे"],
    LanguageCode.TELUGU.value: ["నేను ఆశాను మరియు ఆశా సహేలి బాట్‌లో చేరాలనుకుంటున్నాను"],
}

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
