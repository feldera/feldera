use chrono::{DateTime, Datelike, TimeDelta, Utc};
use fake::Fake;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct Interaction {
    pub interaction_id: u32,
    pub user_id: u32,
    pub video_id: u32,
    pub category_id: u8,
    pub interaction_type: String,
    pub watch_time: u8,
    #[serde(serialize_with = "serialize_timestamp")]
    pub interaction_date: DateTime<Utc>,
    #[serde(serialize_with = "serialize_timestamp")]
    pub interaction_month: DateTime<Utc>,
}

fn serialize_timestamp<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&date.format("%Y-%m-%d %H:%M:%S").to_string())
}

#[derive(Debug, Clone)]
pub struct InteractionGenerator {
    max_users: u32,
    max_videos: u32,
    max_interactions: u32,
    interaction_id: u32,
    start: chrono::DateTime<Utc>,
}

impl Default for InteractionGenerator {
    fn default() -> Self {
        Self {
            max_users: 1_000,
            max_videos: 1_000,
            max_interactions: 10_000_000,
            interaction_id: 0,
            start: Utc::now(),
        }
    }
}

impl InteractionGenerator {
    pub fn max_users(mut self, n: u32) -> Self {
        self.max_users = n;
        self
    }

    pub fn max_videos(mut self, n: u32) -> Self {
        self.max_videos = n;
        self
    }

    pub fn max_interactions(mut self, n: u32) -> Self {
        self.max_interactions = n;
        self
    }

    pub fn start_date(mut self, start: DateTime<Utc>) -> Self {
        self.start = start;
        self
    }

    pub fn generate(mut self) -> Vec<Interaction> {
        let mut rng = rand::thread_rng();

        let mut interactions = Vec::with_capacity(self.max_interactions as usize);

        for _ in 0..self.max_interactions {
            let random_delay = chrono::TimeDelta::seconds(rng.gen_range(0..600));
            let interaction_date = self.timestamp_generator() - random_delay;

            let interaction = Interaction {
                interaction_id: self.interaction_id_generator(),
                user_id: self.user_id_generator(&mut rng),
                video_id: self.video_id_generator(&mut rng),
                category_id: self.video_category_generator(&mut rng),
                interaction_type: self.interaction_type_generator(&mut rng),
                watch_time: self.watch_time_generator(&mut rng),
                interaction_month: interaction_date.with_day(1).unwrap(),
                interaction_date,
            };

            interactions.push(interaction);
        }

        interactions
    }
}

impl InteractionGenerator {
    fn interaction_id_generator(&mut self) -> u32 {
        if self.interaction_id == self.max_interactions {
            self.interaction_id = 0;
        } else {
            self.interaction_id += 1;
        }

        self.interaction_id
    }

    fn user_id_generator(&self, rng: &mut impl Rng) -> u32 {
        Fake::fake_with_rng(&(0..self.max_videos), rng)
    }

    fn video_id_generator(&self, rng: &mut impl Rng) -> u32 {
        Fake::fake_with_rng(&(0..self.max_videos), rng)
    }

    fn interaction_type_generator(&self, rng: &mut impl Rng) -> String {
        let interaction_types = [
            ("like", 1.5),
            ("dislike", 0.2),
            ("view", 3.0),
            ("comment", 0.5),
            ("share", 0.8),
            ("skip", 10.0),
        ];

        interaction_types
            .choose_weighted(rng, |item| item.1)
            .unwrap()
            .0
            .to_string()
    }

    fn video_category_generator(&self, rng: &mut impl Rng) -> u8 {
        Fake::fake_with_rng(&(1..=11), rng)
    }

    fn watch_time_generator(&self, rng: &mut impl Rng) -> u8 {
        Fake::fake_with_rng(&(10..=250), rng)
    }

    fn timestamp_generator(&mut self) -> DateTime<Utc> {
        self.start += TimeDelta::seconds(1);

        self.start
    }
}
