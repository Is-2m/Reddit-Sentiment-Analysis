import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
from typing import Optional, Dict, Union
import logging
import os


class SentimentAnalyzer:
    def __init__(
        self,
        model_name: str = "nlptown/bert-base-multilingual-uncased-sentiment",
        model_dir: str = None,
    ):
        # Get model directory from environment variable if not provided
        # This allows us to override the location through environment variables
        self.model_dir = model_dir or os.getenv("MODEL_PATH", "./saved_model")

        self.model_name = model_name
        # self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = "cpu"

        # Set up logging with a more detailed format to help with debugging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Add a stream handler if none exists
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.max_length = 512
        self.sentiment_analyzer = self._load_model()

    def _load_model(self) -> Optional[pipeline]:
        try:
            # Log the path we're trying to load from
            self.logger.info(f"Attempting to load model from: {self.model_dir}")

            # Check for local model files by looking for config.json
            # This is a reliable way to detect if the model files are present
            config_path = os.path.join(self.model_dir, "config.json")

            if os.path.exists(config_path):
                self.logger.info(
                    "Found existing model files, loading from local storage..."
                )
                tokenizer = AutoTokenizer.from_pretrained(self.model_dir)
                model = AutoModelForSequenceClassification.from_pretrained(
                    self.model_dir
                )
                self.logger.info("Successfully loaded local model files")
            else:
                self.logger.warning(
                    f"No model found at {self.model_dir}, downloading from Hugging Face..."
                )
                # Download from Hugging Face if no local model exists
                tokenizer = AutoTokenizer.from_pretrained(self.model_name)
                model = AutoModelForSequenceClassification.from_pretrained(
                    self.model_name
                )

                # Save the model for future use
                os.makedirs(self.model_dir, exist_ok=True)
                model.save_pretrained(self.model_dir)
                tokenizer.save_pretrained(self.model_dir)
                self.logger.info(f"Downloaded and saved model to {self.model_dir}")

            # Move model to appropriate device (CPU/GPU)
            model = model.to(self.device)
            self.logger.info(f"Model loaded successfully on device: {self.device}")

            return pipeline(
                "sentiment-analysis",
                model=model,
                tokenizer=tokenizer,
                device=0 if self.device == "cuda" else -1,
            )
        except Exception as e:
            self.logger.error(f"Error loading model: {str(e)}", exc_info=True)
            return None

    def _truncate_text(self, text: str) -> str:
        """Truncate text to fit within model's maximum token limit."""
        tokenizer = self.sentiment_analyzer.tokenizer
        tokens = tokenizer.encode(text, add_special_tokens=True)
        if len(tokens) > self.max_length:
            tokens = tokens[: self.max_length - 1] + [tokenizer.sep_token_id]
            return tokenizer.decode(tokens, skip_special_tokens=True)
        return text

    def analyze_batch(self, texts: list, batch_size: int = 16) -> list:
        if not self.sentiment_analyzer:
            raise RuntimeError("Sentiment analyzer not properly initialized")

        results = []
        valid_texts = []

        # Pre-process and validate texts
        for text in texts:
            if not text or not isinstance(text, str):
                self.logger.warning(f"Skipping invalid text: {text}")
                continue
            try:
                truncated_text = self._truncate_text(text)
                valid_texts.append(truncated_text)
            except Exception as e:
                self.logger.warning(f"Error processing text: {e}")
                continue

        # Process batches
        for i in range(0, len(valid_texts), batch_size):
            batch = valid_texts[i : i + batch_size]
            try:
                batch_results = self.sentiment_analyzer(
                    batch,
                    truncation=True,
                    max_length=self.max_length,
                    padding=True,
                )
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
                self.logger.error(f"Error analyzing batch: {e}")
                # Add neutral results for failed batch
                results.extend(
                    [{"text": text, "label": "NEUTRAL", "score": 0.0} for text in batch]
                )

        return results

    def analyze_text(self, text: str) -> Dict[str, Union[str, float]]:
        if not isinstance(text, str):
            self.logger.warning("Invalid input type; expected a string.")
            return {"text": str(text), "label": "NEUTRAL", "score": 0.0}

        try:
            truncated_text = self._truncate_text(text)
            result = self.analyze_batch([truncated_text])[0]
            return result
        except Exception as e:
            self.logger.error(f"Error analyzing text: {e}")
            return {"text": text, "label": "NEUTRAL", "score": 0.0}
