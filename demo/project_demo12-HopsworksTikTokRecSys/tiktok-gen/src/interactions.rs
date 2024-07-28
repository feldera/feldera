use chrono::{DateTime, Datelike, Days, TimeDelta, Utc};
use fake::Fake;
use rand::seq::SliceRandom;
use rand::Rng;
use serde::Serialize;

use crate::users::User;
use crate::videos::Video;

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
    pub previous_interaction_date: DateTime<Utc>,
    #[serde(serialize_with = "serialize_timestamp")]
    pub interaction_month: DateTime<Utc>,
}

fn serialize_timestamp<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&date.format("%Y-%m-%d %H:%M:%S").to_string())
}

impl Interaction {
    pub fn generate(num: u32, users: Vec<User>, videos: Vec<Video>) -> Vec<Interaction> {
        let mut now = Utc::now();
        let mut rng = rand::thread_rng();

        let mut interactions = Vec::with_capacity(num as usize);
        let interaction_types = [
            ("like", 1.5),
            ("dislike", 0.2),
            ("view", 3.0),
            ("comment", 0.5),
            ("share", 0.8),
            ("skip", 10.0),
        ];

        for _ in 0..num {
            let user = users.choose(&mut rng).unwrap();
            let video = videos.choose(&mut rng).unwrap();

            let random_delay = chrono::TimeDelta::seconds(rng.gen_range(0..600));
            let interaction_date = now + random_delay;

            let previous_interaction_date = interaction_date - Days::new(rng.gen_range(0..90));

            let mut watch_time = rng.gen_range(1..video.video_length);

            let probability_watched_till_end = 1.0 - ((watch_time / video.video_length) as f64);

            let watched_till_end = rng.gen_bool(probability_watched_till_end);

            if watched_till_end {
                watch_time = video.video_length;
            }

            interactions.push(Interaction {
                interaction_id: Fake::fake_with_rng(&(100_000_000..999_999_999), &mut rng),
                user_id: user.user_id,
                video_id: video.video_id,
                category_id: video.category_id,
                interaction_type: interaction_types
                    .choose_weighted(&mut rng, |item| item.1)
                    .unwrap()
                    .0
                    .to_string(),
                watch_time,
                interaction_date,
                previous_interaction_date,
                interaction_month: interaction_date.with_day(1).unwrap(),
            });

            now += TimeDelta::seconds(1);
        }

        interactions
    }
}
