//! Wallet-events subscription management (predictWalletEvents/{jwt}).
//!
//! The JWT lives **in the topic string**, and a fresh, unexpired JWT must
//! be used for every (re)subscription — the server checks it at subscribe
//! time (`invalid_credentials` otherwise). This module keeps exactly one
//! wire reference alive across connection generations:
//!
//! - [`WalletTopic`] masks the JWT in `Debug` so it never lands in logs;
//! - [`WalletSubscription`] is the refresh state machine driven by the
//!   consumer loop on every `Ready { generation }` event: release the
//!   stale topic reference, pull a fresh JWT from the injected provider,
//!   subscribe the new topic. Failures keep the stale reference released
//!   (a dead wallet stream is honest; a stale-JWT one is a lie).

use nautilus_core::string::secret::SecretString;

use super::client::PredictWebSocketClient;

/// The wallet-events topic for one JWT (`predictWalletEvents/{jwt}`).
pub struct WalletTopic(String);

impl WalletTopic {
    #[must_use]
    pub fn new(jwt: &SecretString) -> Self {
        Self(format!("predictWalletEvents/{}", jwt.expose_secret()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for WalletTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The topic embeds the bearer token — masked, length-only.
        f.debug_struct("WalletTopic")
            .field("topic_len", &self.0.len())
            .finish()
    }
}

/// One live wallet subscription across reconnect generations.
#[derive(Default)]
pub struct WalletSubscription {
    active: Option<String>,
}

impl WalletSubscription {
    /// The topic currently subscribed (for message filtering), if any.
    #[must_use]
    pub fn active_topic(&self) -> Option<&str> {
        self.active.as_deref()
    }

    /// Re-subscribes with a fresh JWT for the new connection generation.
    ///
    /// `provide_jwt` must resolve to a **fresh** token (the trading
    /// client's authenticate flow); the previously active topic reference
    /// is always released first, and on provider failure nothing is
    /// re-subscribed — the consumer surfaces the gap instead of streaming
    /// on a stale token.
    ///
    /// # Errors
    /// Propagates client disposal/failure or the JWT provider's error.
    pub async fn refresh<F>(
        &mut self,
        client: &PredictWebSocketClient,
        provide_jwt: F,
    ) -> anyhow::Result<()>
    where
        F: std::future::Future<Output = anyhow::Result<SecretString>>,
    {
        // Release the stale reference unconditionally: its JWT may already
        // be expired on the new connection.
        if let Some(stale) = self.active.take() {
            let _ = client.unsubscribe(&stale);
        }
        let jwt = provide_jwt.await?;
        let topic = WalletTopic::new(&jwt);
        client.subscribe(topic.as_str())?;
        self.active = Some(topic.as_str().to_owned());
        // 不回主题串（内嵌 JWT）——调用方过滤经 active_topic()（&str 面
        // 同样含 token，Debug 面安全由 WalletTopic 承担；返回值零暴露）。
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::string::secret::SecretString;

    fn client() -> PredictWebSocketClient {
        let (client, _rx) = PredictWebSocketClient::new(
            "wss://ws.predict.fun/ws".into(),
            &SecretString::from("predict-data-key"),
            5,
            30,
            None,
        );
        client
    }

    /// Debug 面永不携带 JWT（主题内嵌 bearer token——只露长度）。
    #[test]
    fn wallet_topic_debug_masks_jwt() {
        let topic = WalletTopic::new(&SecretString::from("secret-jwt-value"));
        let debug = format!("{topic:?}");
        assert!(!debug.contains("secret-jwt-value"), "{debug}");
        assert!(debug.contains("topic_len"), "{debug}");
        assert!(topic.as_str().starts_with("predictWalletEvents/"));
    }

    /// 刷新状态机：首启 → active；再刷新 → 换到新 token 的主题（旧
    /// 引用释放——active 单值恒一）。
    #[tokio::test]
    async fn refresh_swaps_exactly_one_reference() {
        let client = client();
        let mut sub = WalletSubscription::default();
        sub.refresh(&client, async { Ok(SecretString::from("jwt-one")) })
            .await
            .unwrap();
        assert_eq!(sub.active_topic(), Some("predictWalletEvents/jwt-one"));
        sub.refresh(&client, async { Ok(SecretString::from("jwt-two")) })
            .await
            .unwrap();
        assert_eq!(sub.active_topic(), Some("predictWalletEvents/jwt-two"));
    }

    /// provider 失败：旧引用已释放、无新订阅（诚实空窗——过期 token
    /// 流是谎言）。
    #[tokio::test]
    async fn provider_failure_leaves_no_subscription() {
        let client = client();
        let mut sub = WalletSubscription::default();
        sub.refresh(&client, async { Ok(SecretString::from("jwt-one")) })
            .await
            .unwrap();
        let err = sub
            .refresh(&client, async { Err(anyhow::anyhow!("auth down")) })
            .await;
        assert!(err.is_err());
        assert_eq!(sub.active_topic(), None);
    }
}
