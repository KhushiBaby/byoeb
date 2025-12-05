import re
import time
import os
import pickle
import asyncio
import logging
import hashlib
import xml.etree.ElementTree as ET
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
from byoeb.kb_app.configuration.config import prompt_config
from byoeb.kb_app.configuration.dependency_setup import (
    amedia_storage,
    vector_store,
    azure_openai_embed,
    llm_client
)
from byoeb.services.knowledge_base.kb_service import KBService


async def abulk_download_files(all_files: List[FileMetadata]) -> List[FileData]:
    """Compatibility wrapper that uses KBService's bulk download implementation."""
    svc = KBService(vector_store=vector_store, media_storage=amedia_storage, llm_client=llm_client)
    return await svc._abulk_download_files(all_files)
from azure.identity import DefaultAzureCredential
from typing import List
from datetime import datetime, timezone
from byoeb_core.data_parser.llama_index_text_parser import LLamaIndexTextParser, LLamaIndexTextSplitterType
from byoeb_core.models.media_storage.file_data import FileMetadata, FileData
from enum import Enum
from typing import List
from tqdm.asyncio import tqdm
from datetime import datetime
from byoeb_core.vector_stores.base import BaseVectorStore
from byoeb_core.llms.base import BaseLLM
from azure.search.documents import SearchClient, SearchIndexingBufferedSender
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.models import VectorizableTextQuery, IndexAction
from byoeb_core.models.vector_stores.azure.azure_search import AzureSearchNode, Metadata
from byoeb_integrations.vector_stores.related_questions import aget_related_questions
from byoeb_core.models.vector_stores.chunk import Chunk, Chunk_metadata

logger = logging.getLogger("kb_service")
prefix_raw_documents = "raw_documents"
prefix_raw_faq_documents = "raw_documents/FAQ"
prefix_updated_documents = "expert_update_documents"

## Azure Vector Search properties
endpoint="byoebstage-search"
index_name="byoebstage-doc-index-latest"
key=os.getenv("AZURE_SEARCH_API_KEY")
search_index_client=None

@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
)
async def related_questions_initial_generation(llm_client: BaseLLM, data_chunk):
    def parse_custom_xml(text):
        # Extract content inside <n>...</n>
        n_match = re.search(r"<n>(.*?)</n>", text, re.DOTALL)
        n_value = n_match.group(1).strip() if n_match else None

        # Extract content inside <pairs>...</pairs>
        pairs_match = re.search(r"<pairs>(.*?)</pairs>", text, re.DOTALL)
        pairs_content = pairs_match.group(1).strip() if pairs_match else None
        return n_value, pairs_content
    system_prompt = """
    1. Generate n appropriate questions that an ASHA might ask ASHABot, and corresponding appropriate, complete, and detailed answers to those questions. **n should be between one and five inclusive**. While generating each question-answer pair (pair_1 through pair_n), follow the below rules.  

    <rules> 

    1. is_grounded: each answer MUST be **COMPLETELY GROUNDED** within **ONLY the provided data_chunk**. 

    2. is_selfcontained: each question **MUST** include all necessary context and details so that it can be understood and answered **COMPLETELY** and **APPROPRIATELY** without referencing previous questions or answers. **REMOVE or GENERALIZE all content specific to named individuals**. 

    <eg_selfcontained_questions>  

    <eg1>What are high protein foods for children?</eg1>  

    <eg2>Who should a pregnant teenage girl talk to?</eg2>  

    </eg_selfcontained_questions>  

    <eg_not_selfcontained_questions>  

    <eg1>Tell me more about the side effects of Antara</eg1>  

    <eg2>Who should Mahima talk to?</eg2>  

    </eg_not_selfcontained_questions> 

    3. is_unique: each question-answer pair MUST be **DISTINCT** from the other pairs. Each pair MUST include some information which is not present in the other pairs. Feel free to delete pairs if needed. 

    4. Finally, each question's length MUST be **<character_limit>60</character_limit> CHARACTERS OR LESS**.  Note: **answers do not have a character_limit.** 

    </rules> 

    2. Output n and the question-answer pairs in XML format. Do **NOT** generate any other opening or closing explanations or code.  Sample output for n=5:  

    <output>  

    <n>5</n> 

    <pairs><pair_1><q>First question</q><a>First answer</a></pair_1><pair_2><q>Second question</q><a>Second answer</a></pair_2><pair_3><q>Third question</q><a>Third answer</a></pair_3><pair_4><q>Fourth question</q><a>Fourth answer</a></pair_4><pair_5><q>Fifth question</q><a>Fifth answer</a></pair_5></pairs> 

    </output>  

    </steps> 
    """
    user_prompt = "<data_chunk>CHUNK</data_chunk> "
    user_prompt = user_prompt.replace("CHUNK", data_chunk)
    prompt = [{"role": "system", "content": system_prompt}]
    prompt.append({"role": "user", "content": user_prompt})
    llm_response, resp = await llm_client.agenerate_response(prompt)
    n_value, pairs_content = parse_custom_xml(resp)
    if n_value is None or pairs_content is None:
        raise ValueError("Failed to parse the response from the LLM.")
    return n_value, pairs_content

@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
)
async def related_questions_verification(llm_client: BaseLLM, data_chunk, qa_xml, n):
    def parse_custom_xml(text):
        # Extract content inside <output>...</output>
        pairs_match = re.search(r"<output>(.*?)</output>", text, re.DOTALL)
        pairs_feedback = pairs_match.group(1).strip() if pairs_match else None
        return pairs_feedback
    system_prompt = """<task_description> 

    You are an assistant to a chatbot (called ASHABot) that helps Indian Community Health Workers (called Accredited Social Health Activists or ASHAs) with their queries related to their work in maternal health, child health, and rural health. Your task is to check if the provided n question-answer pairs meet certain rules. Each pair consists of a question that an ASHA might ask ASHABot, and the corresponding answer from ASHABot. 

    </task_description> 

    <steps> 

    1. For each of the provided pairs (pair_1 through pair_n), determine ‘yes’ or ‘no’ whether it follow each of the below rules.  

    <rules> 

    1. is_grounded: i.e., each answer is **GROUNDED** within **ONLY the provided data_chunks**.   

    2. is_selfcontained: each question includes all necessary context and details, so that it can be understood and answered somewhat **COMPLETELY** and **APPROPRIATELY** without referencing previous questions or answers. **There should not be any content specific to named individuals**. 

    <eg_selfcontained_questions> 

    <eg1>What are high protein foods for children?</eg1> 

    <eg2>Who should a pregnant teenage girl talk to?</eg2> 

    </eg_selfcontained_questions> 

    <eg_not_selfcontained_questions> 

    <eg1>Tell me more about the side effects of Antara</eg1> 

    <eg2>Who should Mahima talk to?</eg2> 

    </eg_not_selfcontained_questions> 

    3. is_unique: pair_1 through pair_n are **SOMEWHAT** distinct from each other. While determining is_unique, follow the is_unique_important_considerations below:  

    <is_unique_important_considerations>   

    1. **A pair is unique even if it is a proper subset of another pair.** As long as no two pairs are completely identical, all pairs are unique.

    2. Do **NOT** consider data_chunk while determining uniqueness. **It is IRRELEVANT whether a pair replicates content from data_chunk.**  

    3. If there is just one pair, it is automatically unique.   

    </is_unique_important_considerations> 

    </rules> 

    2. For each pair, if any of is_grounded, is_selfcontained or is_unique is ‘no’, provide a specific explanation in 2-3 concise sentences. Else, leave the explanation blank. 

    3. Output your validations and explanations in XML format. Do **NOT** generate any other opening or closing explanations or code. Sample output for n=5:    

    <output>  

    <pair_1><is_grounded>{yes OR no}</is_grounded><is_selfcontained>{yes OR no}</is_selfcontained><is_unique>{yes OR no}</is_unique><explanation>{explanation}</explanation></pair_1>  

    <pair_2><is_grounded>{yes OR no}</is_grounded><is_selfcontained>{yes OR no</is_selfcontained><is_unique>{yes OR no}</is_unique><explanation>{explanation}</explanation></pair_2>  

    <pair_3><is_grounded>{yes OR no}</is_grounded><is_selfcontained>{yes OR no}</is_selfcontained><is_unique>{yes OR no}</is_unique><explanation>{explanation}</explanation></pair_3>  

    <pair_4><is_grounded>{yes OR no}</is_grounded><is_selfcontained>{yes OR no}</is_selfcontained><is_unique>{yes OR no}</is_unique><explanation>{explanation}</explanation></pair_4>  

    <pair_5><is_grounded>{yes OR no}</is_grounded><is_selfcontained>{yes OR no}</is_selfcontained><is_unique>{yes OR no}</is_unique><explanation>{explanation}</explanation></pair_5>  

    </output>  

    </steps> 
    """

    user_prompt = """
    <data_chunk>CHUNK</data_chunk>\n 
    <n>N</n>
    <pairs>PAIRS</pairs>
    """

    user_prompt = user_prompt.replace("CHUNK", data_chunk).replace("N", str(n)).replace("PAIRS", qa_xml)
    prompt = [{"role": "system", "content": system_prompt}]
    prompt.append({"role": "user", "content": user_prompt})
    llm_response, resp = await llm_client.agenerate_response(prompt)
    pairs_feedback = parse_custom_xml(resp)
    if pairs_feedback is None:
        raise ValueError("Failed to parse the response from the LLM.")
    return pairs_feedback

async def related_questions_generation_feedback(llm_client: BaseLLM, data_chunk, feedback_xml, n):
    def parse_custom_xml(text):
        # Extract content inside <n>...</n>
        n_match = re.search(r"<n>(.*?)</n>", text, re.DOTALL)
        n_value = n_match.group(1).strip() if n_match else None

        # Extract content inside <pairs>...</pairs>
        pairs_match = re.search(r"<pairs>(.*?)</pairs>", text, re.DOTALL)
        pairs_content = pairs_match.group(1).strip() if pairs_match else None
        return n_value, pairs_content
    
    system_prompt = """<task_description> 

    You are an assistant to a chatbot (called ASHABot) that helps Indian Community Health Workers (called Accredited Social Health Activists or ASHAs) with their queries related to their work in maternal health, child health, and rural health. Your task is to appropriately edit/delete the n provided question-answer pairs to ensure they follow certain rules. Each pair consists of a question that an ASHA might ask ASHABot, and the corresponding answer from ASHABot. 

    </task_description> 

    <steps> 

    1. You are provided n question-answer pairs (pair_1 through pair_n), some of which break one or more of the below rules. You are also provided an explanation whenever a pair breaks a rule. Carefully analyze each pair and explanation, and the below rules. 

    <rules> 

    1. is_grounded: each answer MUST be **COMPLETELY GROUNDED** within **ONLY the provided data_chunk**. 

    2. is_selfcontained: each question **MUST** include all necessary context and details so that it can be understood and answered **COMPLETELY** and **APPROPRIATELY** without referencing previous questions or answers. **There should not be any content specific to named individuals.** 

    <eg_selfcontained_questions>  

    <eg1>What are high protein foods for children?</eg1>  

    <eg2>Who should a pregnant teenage girl talk to?</eg2>  

    </eg_selfcontained_questions>  

    <eg_not_selfcontained_questions>  

    <eg1>Tell me more about the side effects of Antara</eg1>  

    <eg2>Who should Mahima talk to?</eg2>  

    </eg_not_selfcontained_questions> 

    3. is_unique: each question-answer pair MUST be **DISTINCT** from the other pairs. Each pair MUST include some information which is not present in the other pairs. 

    4. Finally, each question's length MUST be **<character_limit>60</character_limit> CHARACTERS OR LESS**.  Note: **answers do not have a character_limit.** 

    </rules> 

    2. Carefully edit and/or delete pairs to ensure all pairs follow all the rules. YOU HAVE TO PRESERVE ATLEAST 1 PAIR BECAUSE THAT WILL ALWAYS BE UNIQUE Follow the below instructions: 

    <instructions> 
    YOU HAVE TO PRESERVE ATLEAST 1 PAIR BECAUSE THAT WILL ALWAYS BE UNIQUE
    
    1. If a pair does not break any of the rules, i.e., if is_grounded, is_selfcontained and is_unique are ‘yes’ for that pair: **DO NOT MAKE ANY CHANGES TO IT.** 

    2. If a pair breaks any of the rules, i.e, if is_grounded or is_selfcontained or is_unique is ‘no’ for that pair: **DELETE** the pair, unless you can easily edit the pair **APPROPRIATELY** so that it follows all the rules.   

    3. **DO NOT CREATE ANY ADDITIONAL PAIRS.**
    
    4. If two pairs are identical, delete one of them which is less relevant so that for any chunk we have AT MOST ONE PAIR.

    </instructions>  

    3. If you have deleted any pairs, update n so it is equal to the new total number of pairs.  

    4. Output n and the edited question-answer pairs in XML format. There should be **between one and five inclusive** pairs in your output. Do **NOT** generate any other opening or closing explanations or code.  Sample output with five pairs:  

    <output>  

    <n>5</n> 

    <pairs><pair_1><q>First question</q><a>First answer</a></pair_1><pair_2><q>Second question</q><a>Second answer</a></pair_2><pair_3><q>Third question</q><a>Third answer</a></pair_3><pair_4><q>Fourth question</q><a>Fourth answer</a></pair_4><pair_5><q>Fifth question</q><a>Fifth answer</a></pair_5></pairs>   

    </output>  

    </steps>

    """

    user_prompt = """
    <data_chunk>CHUNK</data_chunk> 
    <n>N</n> 
    <pairs>PAIRS</pairs>
    """

    user_prompt = user_prompt.replace("CHUNK", data_chunk).replace("N", str(n)).replace("PAIRS", feedback_xml)
    prompt = [{"role": "system", "content": system_prompt}]
    prompt.append({"role": "user", "content": user_prompt})
    llm_response, resp = await llm_client.agenerate_response(prompt)
    n_value, pairs_content = parse_custom_xml(resp)
    if n_value is None or pairs_content is None:
        raise ValueError("Failed to parse the response from the LLM.")
    return n_value, pairs_content

def validate_grounded_and_selfcontained(input_text):
    # Wrap in a root tag to parse
    print("Fallback to grounded and selfcontained")
    wrapped_input = f"<root>{input_text}</root>"

    try:
        root = ET.fromstring(wrapped_input)
    except ET.ParseError as e:
        print("XML parsing error:", e)
        return False

    for pair in root:
        try:
            grounded = pair.findtext("is_grounded", "").strip().lower()
            selfcontained = pair.findtext("is_selfcontained", "").strip().lower()
        except AttributeError:
            return False

        if not (grounded == "yes" and selfcontained == "yes"):
            return False

    return True

def validate_all_tags_yes(input_text):
    # Wrap in a root tag to parse
    wrapped_input = f"<root>{input_text}</root>"

    try:
        root = ET.fromstring(wrapped_input)
    except ET.ParseError as e:
        print("XML parsing error:", e)
        return False

    for pair in root:
        try:
            grounded = pair.findtext("is_grounded", "").strip().lower()
            selfcontained = pair.findtext("is_selfcontained", "").strip().lower()
            unique = pair.findtext("is_unique", "").strip().lower()
        except AttributeError:
            return False

        if not (grounded == "yes" and selfcontained == "yes" and unique == "yes"):
            return False

    return True
@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
)
async def agenerate_related_questions(
    data_chunk,
    llm_client: BaseLLM
):
    def extract_questions(xml_input):
        questions = re.findall(r'<q>(.*?)</q>', xml_input)
        return questions
        # Wrap input in a root tag to parse
        wrapped = f"<root>{xml_input}</root>"
        root = ET.fromstring(wrapped)

        questions = []
        for pair in root.findall("./*"):
            q_elem = pair.find("q")
            if q_elem is not None:
                questions.append(q_elem.text.strip())

        return questions
    n_value, pairs_content = await related_questions_initial_generation(
        llm_client=llm_client,
        data_chunk=data_chunk
    )
    valid_pairs_content = pairs_content
    count = 0
    while(True):
        # print(f"Curr pair: {pairs_content}")
        feedback_xml = await related_questions_verification(
            llm_client=llm_client,
            data_chunk=data_chunk,
            qa_xml=pairs_content,
            n=n_value
        )
        # print(feedback_xml)
        if validate_all_tags_yes(feedback_xml):
            # print("All tags are yes")
            # print(pairs_content)
            break
        elif  count < 10 and validate_grounded_and_selfcontained(feedback_xml):
            print(pairs_content)
            break
        if count > 13:
            print("Stuck in loop for 13 iterations")
            print(pairs_content)
            with open("/home/rash598/Khushi/byoeb/byoeb-v1/byoeb/byoeb/scripts/knowledge_base/stuck_chunk.txt", "a") as file:
                print(f"Stuck in loop for {count} iterations")
                file.write(f"{data_chunk}\n")
                file.write(f"{pairs_content}\n")
                file.write(f"{feedback_xml}\n")
            return {
                "en": [],
                "hi": [],
                "mr": [],
                "te": []
            }

        n_value, pairs_content = await related_questions_generation_feedback(
            llm_client=llm_client,
            data_chunk=data_chunk,
            feedback_xml=feedback_xml,
            n=n_value
        )
        valid_pairs_content = pairs_content
        count += 1
    # print(f"Valid pairs content: {valid_pairs_content}")
    related_questions = extract_questions(valid_pairs_content)
    # print(related_questions)
    if related_questions is None:
        raise ValueError("Failed to parse the response from the LLM.")
    related_questions_dict = {}
    related_questions_dict["en"] = related_questions
    languages_translation_prompts = {
        "hi": "You are an english to hindi translator.",
        "mr": "You are an english to marathi translator.",
        "te": "You are an english to telugu translator.",
    }
    for lang, translation_prompt in languages_translation_prompts.items():
        user_prompt = f"""Translate the following list of questions <en_questions> {related_questions} </en_questions> from english to desired language.
        Maintain the output structure as follows: below is example of 3 but it can be any arbitrary number of questions.
        <related_questions>
        <q_1>Translated question 1</q_1>
        <q_2>Translated question 2</q_2>
        <q_3>Translated question 3</q_3>
        </related_questions>
        Note above is a sample for three questions follow same based on number of questions.
        """
        prompt = [{"role": "system", "content": translation_prompt}]
        prompt.append({"role": "user", "content": user_prompt})
        llm_response, resp = await llm_client.agenerate_response(prompt)
        related_questions = re.findall(r"<q_\d+>(.*?)</q_\d+>", resp)
        related_questions_dict[lang] = related_questions
    return related_questions_dict


# generate qas
@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
)
async def agenerate_qas(
    data_chunk,
    llm_client: BaseLLM
):
    def extract_questions(xml_input):
        questions = re.findall(r'<q>(.*?)</q>', xml_input)
        answers = re.findall(r'<a>(.*?)</a>', xml_input)
        return questions, answers
    
    n_value, pairs_content = await related_questions_initial_generation(
        llm_client=llm_client,
        data_chunk=data_chunk
    )
    valid_pairs_content = pairs_content
    count = 0
    while(True):
        # print(f"Curr pair: {pairs_content}")
        feedback_xml = await related_questions_verification(
            llm_client=llm_client,
            data_chunk=data_chunk,
            qa_xml=pairs_content,
            n=n_value
        )
        # print(feedback_xml)
        if validate_all_tags_yes(feedback_xml):
            print("All tags are yes")
            print(pairs_content)
            break
        elif  count < 10 and validate_grounded_and_selfcontained(feedback_xml):
            print(pairs_content)
            break
        if count > 13:
            print("Stuck in loop for 13 iterations")
            print(pairs_content)
            with open("/home/rash598/Khushi/byoeb/byoeb-v1/byoeb/byoeb/scripts/knowledge_base/stuck_chunk.txt", "a") as file:
                print(f"Stuck in loop for {count} iterations")
                file.write(f"{data_chunk}\n")
                file.write(f"{pairs_content}\n")
                file.write(f"{feedback_xml}\n")
            return {}

        n_value, pairs_content = await related_questions_generation_feedback(
            llm_client=llm_client,
            data_chunk=data_chunk,
            feedback_xml=feedback_xml,
            n=n_value
        )
        valid_pairs_content = pairs_content
        count += 1
    print(f"Valid pairs content: {valid_pairs_content}")
    related_questions, related_answers = extract_questions(valid_pairs_content)
    print(related_questions)
    if related_questions is None:
        raise ValueError("Failed to parse the response from the LLM.")
    qa = []
    for question, answer in zip(related_questions, related_answers):
        qa.append({
            "question": question,
            "answer": answer
        })
    return qa
    
abbreviations = {
    "AB-HWCs": "Ayushman Bharat - Health and Wellness Centres",
    "AFC": "Adolescent Friendly Club",
    "AFHCs": "Adolescent Friendly Health Clinics",
    "AFHS": "Adolescent Friendly Health Services",
    "AHWD": "Adolescent Health and Wellness Day",
    "AIDS": "Acquired Immune Deficiency Syndrome",
    "ANM": "Auxiliary Nurse Midwife",
    "ART": "Antiretroviral Therapy",
    "ASHAs": "Accredited Social Health Activists",
    "AWC": "Anganwadi Centre",
    "AWW": "Anganwadi Worker",
    "BMI": "Body Mass Index",
    "CAB": "COVID Appropriate Behaviour",
    "CEDAW": "Convention on the Elimination of All Forms of Discrimination Against Women",
    "CPO": "Child Protection Officer",
    "CRC": "Convention on the Rights of the Child",
    "ECPs": "Emergency Contraceptive Pills",
    "FLWs": "Frontline Workers",
    "GBV": "Gender-based Violence",
    "GoI": "Government of India",
    "HCWs": "Healthcare Workers",
    "HIV": "Human Immunodeficiency Virus",
    "IFA": "Iron and Folic Acid",
    "IUCD": "Intra-Uterine Contraceptive Device",
    "IUDs": "Intra-Uterine Devices",
    "MHS": "Menstrual Hygiene Scheme",
    "MoHFW": "Ministry of Health & Family Welfare",
    "MTP": "Medical Termination of Pregnancy",
    "NCDs": "Non-Communicable Diseases",
    "NCPCR": "National Commission for Protection of Child Rights",
    "NHM": "National Health Mission",
    "NTQLS": "National Tobacco Quit Line Services",
    "OCPs": "Oral Contraceptive Pills",
    "PE": "Peer Education/Educator",
    "PID": "Pelvic Inflammatory Disease",
    "POSCO Act, 2012": "Protection of Children from Sexual Offences Act, 2012",
    "PPTCT": "Prevention of Parent to Child Transmission",
    "RKSK": "Rashtriya Kishor Swasthya Karyakram",
    "RTIs": "Reproductive Tract Infections",
    "SRH": "Sexual and Reproductive Health",
    "STIs": "Sexually Transmitted Infections",
    "VHSNC": "Village Health, Sanitation and Nutrition Committee",
    "WIFS": "Weekly Iron Folic Acid Supplementation",
    "CS": "Caesarean Section"
}
def replace_abbreviations(text, abbrev_dict):
    # Sort by length of abbreviation to avoid partial matches
    for abbr in sorted(abbrev_dict, key=len, reverse=True):
        pattern = r'\b' + re.escape(abbr) + r'\b'
        replacement = f"{abbr} ({abbrev_dict[abbr]})"
        text = re.sub(pattern, replacement, text)
    return text

def parse_type1(content):   
    # Split before lines that start with 1–2 digits, optional dot, then tab
    parts = re.split(r'(?=^\d{1,2}\.?\t)', content, flags=re.MULTILINE)

    # Clean and filter out any empty or malformed parts
    faqs = [part.strip() for part in parts if part.strip()]
    
    return faqs

def parse_type2(content):
    qa_pairs = re.findall(r'(?m)^(Q\d+)\.\s+(.*?)\n(.*?)(?=(?:\nQ\d+\.\s)|\Z)', content, re.DOTALL)
    return [q.strip() + a.strip() for _, q, a in qa_pairs]

def parse_type3(content):
    segments = [seg.strip() for seg in content.strip().split('\n\n') if seg.strip()]
    return segments
    qa_pairs = []
    for i in range(0, len(segments) - 1, 2):
        question = segments[i].replace('\n', ' ').strip()
        answer = segments[i+1].replace('\n', ' ').strip()
        qa_pairs.append({'question': question, 'answer': answer})
    return qa_pairs

def detect_type(content):
    # Match full lines that begin with a digit and a dot, with a short question (not long paragraphs)
    type2_matches = re.findall(r'(?m)^Q\d+\.\s+[^\n]{5,100}$', content)
    if type2_matches:
        return 2
    else:
        return 3

def process_file(content, file_name):
    file_type = detect_type(content)
    if "tuberculosis" in file_name:
        chunks = parse_type1(content)
        return chunks 
    if file_type == 1:
        return parse_type1(content)
    elif file_type == 2:
        return parse_type2(content)
    else:
        return parse_type3(content)
    
async def create_raw_files_chunks(files: list):
    files_data = await abulk_download_files(files)
    text_parser = LLamaIndexTextParser(
        chunk_size=300,
        chunk_overlap=50,
    )
    chunks = text_parser.get_chunks_from_collection(
        files_data,
        splitter_type=LLamaIndexTextSplitterType.SENTENCE
    )
    chunk_texts = [chunk.text for chunk in tqdm(chunks, desc="Extracting raw chunk texts")]
    chunk_metadatas = [
        {
            "source": chunk.metadata["file_name"],
            "creation_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            "update_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
        }
        for chunk in tqdm(chunks, desc="Generating raw metadata")
    ]
    chunk_ids = [hashlib.md5(chunk.text.encode()).hexdigest() for chunk in tqdm(chunks, desc="Hashing raw chunk texts")]
    return chunk_ids, chunk_texts, chunk_metadatas

async def create_update_files_chunk(files: list):
    delimiter = "##"
    metadatas, texts = [], []
    files_data = await abulk_download_files(files)
    if isinstance(texts, list) and all(isinstance(item, FileData) for item in texts):
        texts = [d.data.decode("utf-8") for d in files_data]
        metadatas = [d.metadata.model_dump() for d in files_data]
    else:
        raise ValueError("Invalid data")
    
    chunk_ids, chunk_texts, chunk_metadatas = [], [], []
    for text, metadata in tqdm(zip(texts, metadatas), total=len(texts), desc="preparing update files"):
        sections = [section.strip() for section in text.split(delimiter) if section.strip()]
        for section in sections:
            chunk_id = hashlib.md5(section.encode()).hexdigest()
            chunk_text = section
            chunk_metadata = {
                "source": metadata["file_name"],
                "creation_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
                "update_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            }
            chunk_ids.append(chunk_id)
            chunk_texts.append(chunk_text)
            chunk_metadatas.append(chunk_metadata)
    return chunk_ids, chunk_texts, chunk_metadatas

async def create_faq_files_chunk(files: list):
    files_data = await abulk_download_files(files)
    metadatas, texts = [], []
    if isinstance(texts, list) and all(isinstance(item, FileData) for item in texts):
        texts = [d.data.decode("utf-8") for d in files_data]
        metadatas = [d.metadata.model_dump() for d in files_data]
    else:
        raise ValueError("Invalid data")

    chunk_ids, chunk_texts, chunk_metadatas = [], [], []
    for text, metadata in tqdm(zip(texts, metadatas), total=len(texts), desc="Preparing faq files"):
        # print(detect_type(text)) 
        chunks = process_file(text, metadata['file_name'])
        chunks = [replace_abbreviations(t, abbreviations) for t in chunks]
        for chunk in chunks:
            chunk_id = hashlib.md5(chunk.encode()).hexdigest()
            chunk_text = chunk
            chunk_metadata = {
                "source": metadata["file_name"],
                "creation_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
                "update_timestamp": str(int(datetime.now(timezone.utc).timestamp())),
            }
            chunk_ids.append(chunk_id)
            chunk_texts.append(chunk_text)
            chunk_metadatas.append(chunk_metadata)
    return chunk_ids, chunk_texts, chunk_metadatas

# in use
@retry(
    stop=stop_after_attempt(3),  # Retry up to 3 times
    wait=wait_exponential(multiplier=1, max=10),  # Exponential backoff with a max wait time of 10 seconds
)
async def prepare_azure_node(
        id,
        chunk,
        metadata,
        llm_client: BaseLLM
    ) -> AzureSearchNode:
        related_questions = {}
        related_questions = await agenerate_related_questions(
            chunk,
            llm_client
        )
        print(f"Related questions for chunk {id}: {related_questions}")
        try:
            text_vector_3072=await azure_openai_embed.get_embedding_function().aget_text_embedding(chunk)
            print(f"Generated embedding for chunk {id}: {len(text_vector_3072)} dimensions")
        except Exception as e:
            print(f"Error generating embedding for chunk {id}: {e}")
            raise e
        azure_doc = AzureSearchNode(
            id=id,
            text=chunk,
            metadata=metadata,
            text_vector_3072=text_vector_3072,
            related_questions=related_questions,
        )
        return azure_doc

async def prepare_azure_nodes(
    ids,
    data_chunks,
    metadata,
    batch_size=5,
    llm_client: BaseLLM = None
):
    documents = []
    pkl_path = "/home/rash598/Khushi/byoeb/byoeb-v1/byoeb/byoeb/scripts/knowledge_base/temp.pkl"
    try:
        with open(pkl_path, "rb") as file:
            documents = pickle.load(file)
    except:
        documents = []
        with open(pkl_path, "wb") as file:
            pickle.dump(documents, file)
        print(f"Created checkpoint file")
    start = len(documents)
    print(f"Starting from chunk {start}")
    checkpoint = 5
    total_batches = (len(data_chunks) + batch_size - 1) // batch_size  # Calculate total batches

    # Initialize tqdm progress bar if enabled
    curr = 0
    for i in tqdm(range(start, len(data_chunks), batch_size), desc="Processing batches"):
        batch_chunks = data_chunks[i:i+batch_size]
        batch_ids = ids[i:i+batch_size]
        batch_metadata = metadata[i:i+batch_size]

        # Process batch concurrently
        try:
            batch_nodes = await asyncio.gather(*[
                prepare_azure_node(
                    id=batch_ids[idx],
                    chunk=batch_chunks[idx],
                    metadata=batch_metadata[idx],
                    llm_client=llm_client
                ) for idx in range(len(batch_chunks))
            ])
            current_documents = [node.model_dump(exclude_none=True, exclude_defaults=True) for node in batch_nodes]
            documents.extend(current_documents)
            # print(f"Processed {i+batch_size} out of {len(data_chunks)} chunks")
            if curr == checkpoint:
                curr = 0
                print(f"Saving checkpoint... {i+batch_size}/{len(data_chunks)}")
                with open(pkl_path, "wb") as file:
                    pickle.dump(documents, file)
            curr += 1
            time.sleep(0.5)
        except Exception as e:
            print(batch_chunks)
            print(e)
            raise e
    print(f"Saving checkpoint...")
    with open(pkl_path, "wb") as file:
        pickle.dump(documents, file)
    return documents

async def aget_files_from_blob_store():
    files = await amedia_storage.aget_all_files_properties()
    raw_files = [file for file in files if prefix_raw_documents in file.file_name]
    faq_files = [file for file in raw_files if "faq" in file.file_name.lower()]  # Fixed `to_lower()` to `lower()`
    raw_files_without_faq = [file for file in raw_files if file not in faq_files]  # Corrected subtraction
    update_files = [file for file in files if prefix_updated_documents in file.file_name]
    return faq_files, raw_files_without_faq, update_files
    
def fails(error: IndexAction):
        print("Failed to upload document")
        with open("/home/rash598/Khushi/byoeb/byoeb-v1/byoeb/byoeb/scripts/knowledge_base/failures.txt", "a") as file:
            file.write(f"Action type: {error.action_type}\n")
            file.write(f"Properties: {error.additional_properties}\n")
            
def upload_documents(documents: List[AzureSearchNode]):
    batch_size = 10
    for i in tqdm(range(0, len(documents), batch_size)):
        batch_documents = documents[i:i + batch_size]
        # current_documents = [doc.model_dump(exclude_none=True, exclude_defaults=True) for doc in batch_documents]
        search_index_client.upload_documents(documents=batch_documents)
            
async def main():
    # Use KBService for ingestion (delegates to configured vector_store)
    svc = KBService(vector_store=vector_store, media_storage=amedia_storage, llm_client=llm_client)
    try:
        files = await amedia_storage.aget_all_files_properties()
        count = await svc.upload(files)
        logger.info(f"✅ KB ingestion complete - {count} chunks ingested")
    finally:
        # Close media storage if it exposes a close method
        try:
            await amedia_storage._close()
        except Exception:
            pass
    

if __name__ == "__main__":
    asyncio.run(main())