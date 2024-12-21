import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
from typing import Optional, Dict, Union
import logging

class SentimentAnalyzer:
    def __init__(self, model_name: str = 'nlptown/bert-base-multilingual-uncased-sentiment'):
        self.model_name = model_name
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        self.sentiment_analyzer = self._load_model()
        self.logger = logging.getLogger(__name__)

    def _load_model(self) -> Optional[pipeline]:
        try:
            tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            model = model.to(self.device)
            
            return pipeline(
                "sentiment-analysis",
                model=model,
                tokenizer=tokenizer,
                device=0 if self.device == 'cuda' else -1
            )
        except Exception as e:
            self.logger.error(f"Error loading model: {e}")
            return None

    def analyze_batch(self, texts: list) -> list:
        if not self.sentiment_analyzer:
            raise RuntimeError("Sentiment analyzer not properly initialized")
        
        try:
            results = self.sentiment_analyzer(texts)
            return [
                {
                    'text': text,
                    'label': result['label'],
                    'score': float(result['score'])
                }
                for text, result in zip(texts, results)
            ]
        except Exception as e:
            self.logger.error(f"Error in batch analysis: {e}")
            return []

    def analyze_text(self, text: str) -> Dict[str, Union[str, float]]:
        if not text or not isinstance(text, str):
            return {'text': text, 'label': 'NEUTRAL', 'score': 0.0}
            
        try:
            result = self.analyze_batch([text])[0]
            return result
        except Exception as e:
            self.logger.error(f"Error analyzing text: {e}")
            return {'text': text, 'label': 'ERROR', 'score': 0.0}