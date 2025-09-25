
from langchain_core.language_models import BaseLLM
from langchain_community.llms.ollama import Ollama
import itertools

class RotatingOllamaLLM(BaseLLM):
    def __init__(self, base_urls):
        self.clients = [Ollama(base_url=url, model='llama3') for url in base_urls]
        self.cycle = itertools.cycle(self.clients)

    def _call(self, prompt, **kwargs):
        client = next(self.cycle)
        return client.invoke(prompt)

