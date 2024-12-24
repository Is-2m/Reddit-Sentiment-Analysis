import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
from typing import Optional, Dict, Union
import logging


class SentimentAnalyzer:
    def __init__(
        self, model_name: str = "nlptown/bert-base-multilingual-uncased-sentiment"
    ):
        self.model_name = model_name
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.sentiment_analyzer = self._load_model()

    def _load_model(self) -> Optional[pipeline]:
        try:
            tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
            model = model.to(self.device)

            self.logger.info(
                f"Model '{self.model_name}' loaded successfully on device '{self.device}'"
            )
            return pipeline(
                "sentiment-analysis",
                model=model,
                tokenizer=tokenizer,
                device=0 if self.device == "cuda" else -1,
            )
        except Exception as e:
            self.logger.error("Error loading model", exc_info=True)
            return None

    def analyze_batch(self, texts: list, batch_size: int = 16) -> list:
        if not self.sentiment_analyzer:
            raise RuntimeError("Sentiment analyzer not properly initialized")

        valid_texts = [text for text in texts if text and isinstance(text, str)]
        results = []

        for i in range(0, len(valid_texts), batch_size):
            batch = valid_texts[i : i + batch_size]
            try:
                batch_results = self.sentiment_analyzer(batch)
                results.extend(
                    [
                        {
                            "text": text,
                            "label": result["label"],
                            "score": float(result["score"]),
                        }
                        for text, result in zip(batch, batch_results)
                    ]
                )
            except Exception as e:
                self.logger.error(f"Error analyzing batch: {e}", exc_info=True)
        return results

    def analyze_text(self, text: str) -> Dict[str, Union[str, float]]:
        if not isinstance(text, str):
            self.logger.warning("Invalid input type; expected a string.")
            return {"text": str(text), "label": "NEUTRAL", "score": 0.0}

        try:
            result = self.analyze_batch([text])[0]
            return result
        except Exception as e:
            self.logger.error(f"Error analyzing text: {e}", exc_info=True)
            return {"text": text, "label": "ERROR", "score": 0.0}
