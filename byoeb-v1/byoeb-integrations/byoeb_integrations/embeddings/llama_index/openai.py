from typing import Optional
from llama_index.embeddings.openai import OpenAIEmbedding
from byoeb_core.embeddings.base import BaseEmbedding

class OpenAIEmbed(BaseEmbedding):
    def __init__(
            self,
            model: str,
            dimensions: Optional[int] =None,
            api_key: Optional[str] = None,
            **kwargs
        ) -> None:
            embedding_fn = None
            if model is None:
                raise ValueError("model must be provided")
            if api_key is None:
                raise ValueError("api_key must be provided")
            embedding_fn = OpenAIEmbedding(
                model=model,
                dimensions=dimensions,
                api_key=api_key,
                reuse_client=False
            )
            
            self.__embedding_fn = embedding_fn

    def get_embedding_function(self):
        return self.__embedding_fn