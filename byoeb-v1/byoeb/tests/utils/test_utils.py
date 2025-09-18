import pytest
from byoeb.utils.utils import is_onboard

@pytest.mark.parametrize("text,lang,expected", [
    # english tests
    ("onboard-asha", "en", True),  # "-" is treated as " "
    ("Need to onboard asha quickly", "en", True),
    ("ONBOARD ASHA", "en", True),  # case insensitive test
    ("this isOnboard-asha test", "en", True),  # substring test
    ("onboard", "en", False),  # partial phrase test
    ("random text", "en", False),

    # hindi tests
    ("में एक आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है", "hi", True),
    ("  में एक आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है  ", "hi", True),  # with space
    ("नमस्ते", "hi", False),

    # marathi tests
    ("मी आशा आहे आणि मला आशा सहेली बॉटमध्ये सामील व्हायचे आहे", "mr", True),
    ("मी आशा आहे", "mr", False),  # partial phrase
    ("मराठी random", "mr", False),

    # telugu tests
    ("నేను ఆశాను మరియు ఆశా సహేలి బాట్‌లో చేరాలనుకుంటున్నాను", "te", True),
    ("text... నేను ఆశాను మరియు ఆశా సహేలి బాట్‌లో చేరాలనుకుంటున్నాను ...text", "te", True),
    ("తెలుగు random", "te", False),

    # other cases
    ("", "en", False),
    ("    ", "en", False),
    ("onboard-asha", "fr", False),  # unsupported lang always returns False
    ("में एक आशा हूँ और मुझे आशा सहेली बोट से जुड़ना है", "fr", False),  # unsupported lang with valid text
    ("नमस्ते\u2028", "hi", False),  # line separator U+2028 should not affect logic
    ("onboard%20asha", "en", True),  # url-encoded string
    ("में%20एक%20आशा%20हूँ%20और%20मुझे%20आशा%20सहेली%20बोट%20से%20जुड़ना%20है", "hi", True),  # multiple url-encoded chars
    ("%E0%A4%AE%E0%A5%87%E0%A4%82%20%E0%A4%8F%E0%A4%95%20%E0%A4%86%E0%A4%B6%E0%A4%BE%20%E0%A4%B9%E0%A5%82%E0%A4%81%20%E0%A4%94%E0%A4%B0%20%E0%A4%AE%E0%A5%81%E0%A4%9D%E0%A5%87%20%E0%A4%86%E0%A4%B6%E0%A4%BE%20%E0%A4%B8%E0%A4%B9%E0%A5%87%E0%A4%B2%E0%A5%80%20%E0%A4%AC%E0%A5%8B%E0%A4%9F%20%E0%A4%B8%E0%A5%87%20%E0%A4%9C%E0%A5%81%E0%A5%9C%E0%A4%A8%E0%A4%BE%20%E0%A4%B9%E0%A5%88", "hi", True)  # all characters are non-ascii (the string is fully encoded)
])
def test_is_onboard(text, lang, expected):
    assert is_onboard(text, lang) == expected
