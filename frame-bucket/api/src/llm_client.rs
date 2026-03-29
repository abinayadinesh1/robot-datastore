use reqwest::multipart;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{info, warn};

const MAX_RETRIES: u32 = 2;
const RETRY_DELAYS: [u64; 2] = [1, 3];

#[derive(Debug)]
pub enum LlmError {
    Http(String),
    Parse(String),
}

impl std::fmt::Display for LlmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LlmError::Http(e) => write!(f, "LLM HTTP error: {e}"),
            LlmError::Parse(e) => write!(f, "LLM parse error: {e}"),
        }
    }
}

// ---------------------------------------------------------------------------
// OpenAI-compatible chat completion types (for vLLM text endpoint)
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
}

#[derive(Serialize)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatChoiceMessage,
}

#[derive(Deserialize)]
struct ChatChoiceMessage {
    content: String,
}

// ---------------------------------------------------------------------------
// Video chat response (Modal VideoChat-Flash endpoint)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct VideoChatResponse {
    response: String,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

pub struct LlmClient {
    client: reqwest::Client,
    /// vLLM text endpoint base URL (e.g. https://...modal.run)
    llm_base_url: String,
    /// Model name/alias to use with the vLLM server
    llm_model: String,
    /// VideoChat-Flash endpoint base URL
    video_base_url: String,
}

impl LlmClient {
    pub fn new(llm_base_url: String, llm_model: String, video_base_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("failed to build reqwest client");
        Self {
            client,
            llm_base_url,
            llm_model,
            video_base_url,
        }
    }

    /// POST to /v1/chat/completions (OpenAI-compatible vLLM endpoint).
    /// Used for both summarization and path-choice — the caller constructs
    /// the appropriate prompt.
    async fn llm_chat(&self, prompt: &str) -> Result<String, LlmError> {
        let url = format!("{}/v1/chat/completions", self.llm_base_url);

        let body = ChatCompletionRequest {
            model: self.llm_model.clone(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
            temperature: Some(0.0),
            max_tokens: Some(1024),
        };

        for attempt in 0..=MAX_RETRIES {
            match self.client.post(&url).json(&body).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let resp_body = resp.text().await.unwrap_or_default();
                        if attempt < MAX_RETRIES {
                            warn!(attempt, %status, "llm_chat failed, retrying");
                            tokio::time::sleep(Duration::from_secs(RETRY_DELAYS[attempt as usize]))
                                .await;
                            continue;
                        }
                        return Err(LlmError::Http(format!("status {status}: {resp_body}")));
                    }
                    let parsed: ChatCompletionResponse = resp
                        .json()
                        .await
                        .map_err(|e| LlmError::Parse(e.to_string()))?;
                    let content = parsed
                        .choices
                        .into_iter()
                        .next()
                        .map(|c| c.message.content)
                        .unwrap_or_default();
                    return Ok(content);
                }
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        warn!(attempt, error = %e, "llm_chat request failed, retrying");
                        tokio::time::sleep(Duration::from_secs(RETRY_DELAYS[attempt as usize]))
                            .await;
                        continue;
                    }
                    return Err(LlmError::Http(e.to_string()));
                }
            }
        }
        unreachable!()
    }

    /// Summarize multiple child descriptions into a single parent summary.
    pub async fn summarize_text(&self, child_summaries: &[&str]) -> Result<String, LlmError> {
        let mut prompt = String::from(
            "Summarize the following descriptions of consecutive video segments into a single \
             cohesive paragraph that captures the key activities and events. Keep the summary \
             under 200 words.\n\nSegment descriptions:\n",
        );
        for (i, s) in child_summaries.iter().enumerate() {
            prompt.push_str(&format!("---\nSegment {}:\n{}\n", i + 1, s));
        }
        prompt.push_str("---\n\nSummary:");

        info!(child_count = child_summaries.len(), "summarize_text call");
        self.llm_chat(&prompt).await
    }

    /// Ask the LLM to assign probabilities to each child path given a question.
    /// Returns Vec<(child_index, probability)> sorted by probability descending.
    pub async fn choose_path(
        &self,
        question: &str,
        children_summaries: &[(usize, &str)],
    ) -> Result<Vec<(usize, f64)>, LlmError> {
        let mut prompt = format!(
            "You are analyzing video segments to answer a user's question. Given the question \
             and summaries of different time periods, determine which time period is most likely \
             to contain the answer.\n\n\
             Question: {question}\n\n\
             Time periods:\n"
        );
        for (idx, summary) in children_summaries {
            prompt.push_str(&format!("[{idx}]: {summary}\n\n"));
        }
        prompt.push_str(
            "Output ONLY a JSON array with a probability for each time period. \
             Probabilities must sum to 1.0. Example format:\n\
             [{\"index\": 0, \"probability\": 0.7}, {\"index\": 1, \"probability\": 0.3}]\n\n\
             JSON:",
        );

        let response = self.llm_chat(&prompt).await?;
        parse_probabilities(&response, children_summaries)
    }

    /// POST multipart to /v1/video/chat with a video file and question.
    /// This hits the VideoChat-Flash Modal endpoint (separate from the vLLM text model).
    pub async fn ask_video(
        &self,
        video_bytes: Vec<u8>,
        question: &str,
        num_frames: u32,
    ) -> Result<String, LlmError> {
        let url = format!("{}/v1/video/chat", self.video_base_url);

        for attempt in 0..=MAX_RETRIES {
            let file_part = multipart::Part::bytes(video_bytes.clone())
                .file_name("segment.mp4")
                .mime_str("video/mp4")
                .map_err(|e| LlmError::Http(e.to_string()))?;

            let form = multipart::Form::new()
                .part("file", file_part)
                .text("question", question.to_string())
                .text("max_num_frames", num_frames.to_string());

            match self.client.post(&url).multipart(form).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        if attempt < MAX_RETRIES {
                            warn!(attempt, %status, "ask_video failed, retrying");
                            tokio::time::sleep(Duration::from_secs(
                                RETRY_DELAYS[attempt as usize],
                            ))
                            .await;
                            continue;
                        }
                        return Err(LlmError::Http(format!("status {status}: {body}")));
                    }
                    let parsed: VideoChatResponse = resp
                        .json()
                        .await
                        .map_err(|e| LlmError::Parse(e.to_string()))?;
                    return Ok(parsed.response);
                }
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        warn!(attempt, error = %e, "ask_video request failed, retrying");
                        tokio::time::sleep(Duration::from_secs(
                            RETRY_DELAYS[attempt as usize],
                        ))
                        .await;
                        continue;
                    }
                    return Err(LlmError::Http(e.to_string()));
                }
            }
        }
        unreachable!()
    }
}

/// Parse the LLM's JSON probability response. Falls back to uniform distribution.
fn parse_probabilities(
    response: &str,
    children: &[(usize, &str)],
) -> Result<Vec<(usize, f64)>, LlmError> {
    // Try to find a JSON array in the response
    let trimmed = response.trim();

    // Look for [...] in the response
    let json_str = if let Some(start) = trimmed.find('[') {
        if let Some(end) = trimmed.rfind(']') {
            &trimmed[start..=end]
        } else {
            trimmed
        }
    } else {
        trimmed
    };

    #[derive(Deserialize)]
    struct ProbEntry {
        index: usize,
        probability: f64,
    }

    match serde_json::from_str::<Vec<ProbEntry>>(json_str) {
        Ok(entries) => {
            let mut result: Vec<(usize, f64)> = entries
                .into_iter()
                .map(|e| (e.index, e.probability))
                .collect();
            result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            Ok(result)
        }
        Err(e) => {
            warn!(
                error = %e,
                response = %response,
                "failed to parse LLM probabilities, using uniform"
            );
            let n = children.len() as f64;
            let uniform = 1.0 / n;
            let result: Vec<(usize, f64)> =
                children.iter().map(|(idx, _)| (*idx, uniform)).collect();
            Ok(result)
        }
    }
}
