import logging
from typing import List
from byoeb_core.models.media_storage.file_data import FileData, FileMetadata
from enum import Enum
from llama_index.core.schema import BaseNode, TextNode, Document
from llama_index.core.text_splitter import SentenceSplitter
from llama_index.core.node_parser import TokenTextSplitter

logger = logging.getLogger(__name__)

class LLamaIndexTextSplitterType(Enum):
    SENTENCE = "sentence"
    SEMANTIC_DOUBLE_MERGING = "semantic_double_merging"
    TOKEN_TEXT_SPLITTER = "token_text_splitter"

class LLamaIndexTextParser:
    def __init__(
        self,
        chunk_size: int = 256,
        chunk_overlap: int = 10,
        separator: str = " "
    ):
        self._chunk_size = chunk_size
        self._chunk_overlap = chunk_overlap
        self._separator = separator

    def get_sentence_splitter(
        self,
    ) -> SentenceSplitter:
        return SentenceSplitter(
            chunk_size=self._chunk_size,
            chunk_overlap=self._chunk_overlap,
            separator=self._separator
        )
    
    def get_token_text_splitter(
        self,
    ) -> TokenTextSplitter:
        return TokenTextSplitter(
            chunk_size=self._chunk_size,
            chunk_overlap=self._chunk_overlap,
            separator=self._separator
        )
    
    def get_splitter(
        self,
        type
    ):
        if type == LLamaIndexTextSplitterType.SENTENCE:
            return self.get_sentence_splitter()
        elif type == LLamaIndexTextSplitterType.TOKEN_TEXT_SPLITTER:
            return self.get_token_text_splitter()
        else:
            raise ValueError("Invalid type")
    
    def get_chunks_from_collection(
        self,
        data: List[str] | List[FileData],
        encoding: str = "utf-8",
        splitter_type=LLamaIndexTextSplitterType.SENTENCE
    ) -> List[BaseNode]:
        logger.info(f"🔤 Parsing {len(data)} items into chunks (encoding: {encoding})")
        metadatas = []
        texts = data
        if isinstance(texts, list) and all(isinstance(item, FileData) for item in texts):
            # Try to decode with multiple encodings if utf-8 fails
            texts = []
            for idx, d in enumerate(data):
                assert isinstance(d, FileData)
                file_name = d.metadata.file_name if d.metadata else f"file_{idx}"
                logger.debug(f"  Decoding file {idx+1}/{len(data)}: {file_name}")
                
                try:
                    text = d.data.decode(encoding)
                    logger.debug(f"  ✅ Successfully decoded {file_name} with {encoding}")
                except UnicodeDecodeError as e:
                    logger.warning(f"  ⚠️  UTF-8 decode failed for {file_name} at position {e.start}: {str(e)}")
                    # Try common encodings as fallback
                    decoded = False
                    for fallback_encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
                        try:
                            text = d.data.decode(fallback_encoding)
                            logger.info(f"  ✅ Successfully decoded {file_name} with fallback encoding: {fallback_encoding}")
                            decoded = True
                            break
                        except UnicodeDecodeError:
                            continue
                    
                    if not decoded:
                        # If all encodings fail, use errors='replace' to replace invalid bytes
                        logger.warning(f"  ⚠️  All encodings failed for {file_name}, using errors='replace'")
                        text = d.data.decode(encoding, errors='replace')
                
                texts.append(text)
            metadatas = [d.metadata.model_dump() for d in data]
            logger.info(f"✅ Successfully decoded {len(texts)} files")
        else:
            logger.info("Processing string data (not FileData objects)")
            metadatas = [{} for _ in data]
        
        logger.info(f"📝 Creating documents from {len(texts)} texts")
        documents = [Document(text=text, metadata=metadata) for text, metadata in zip(texts, metadatas)]
        
        logger.info(f"✂️  Splitting documents into chunks using {splitter_type.value}")
        splitter = self.get_splitter(splitter_type)
        nodes = splitter.get_nodes_from_documents(documents)
        
        # Log chunking details per file
        if isinstance(data, list) and all(isinstance(item, FileData) for item in data):
            from collections import defaultdict
            chunks_per_file = defaultdict(int)
            for node in nodes:
                file_name = node.metadata.get("file_name", "unknown")
                chunks_per_file[file_name] += 1
            
            logger.info(f"📊 Chunking summary by file:")
            for file_name, chunk_count in sorted(chunks_per_file.items()):
                logger.info(f"  📄 {file_name}: {chunk_count} chunks")
        
        logger.info(f"✅ Created {len(nodes)} chunks from {len(documents)} documents")
        return nodes
    
    def get_chunks_from_text(
        self,
        data: str | FileData,
        encoding: str = "utf-8",
        splitter_type=LLamaIndexTextSplitterType.SENTENCE
    ) -> List[TextNode]:
        metadata = {}
        text = data
        if isinstance(data, FileData):
            try:
                text = data.data.decode(encoding)
            except UnicodeDecodeError:
                # Try common encodings as fallback
                for fallback_encoding in ['latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        text = data.data.decode(fallback_encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    # If all encodings fail, use errors='replace' to replace invalid bytes
                    text = data.data.decode(encoding, errors='replace')
            metadata = data.metadata.model_dump()
        document = Document(
            text=text,
            metadata=metadata
        )
        splitter = self.get_splitter(splitter_type)
        nodes = splitter.get_nodes_from_documents([document])
        return nodes


if __name__ == "__main__":
    text_parser = LLamaIndexTextParser(chunk_size=50, chunk_overlap=1)
    file_data_1 = FileData(
        data=b"This is a test sentence. This is another test sentence.",
        metadata=FileMetadata(
            file_name="abc.txt",
            file_type=".txt",
            creation_time="2021-09-01T00:00:00Z"
        )
    )
    file_data_2 = FileData(
        data=b"How are you doing? I am doing well.",
        metadata=FileMetadata(
            file_name="xyz.txt",
            file_type=".txt",
            creation_time="2021-09-01T00:00:00Z"
        )
    )
    text_1 = "This is a test sentence. This is another test sentence."
    text_2 = "How are you doing? I am doing well."
    chunks = text_parser.get_chunks_from_collection([text_1,text_2])
    print(chunks)
