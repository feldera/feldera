use std::collections::HashMap;

use chrono::{DateTime, Days, Utc};
use fake::{Dummy, Fake, Faker};
use rand::seq::SliceRandom;
use rand::Rng;

#[derive(Debug)]
pub struct Video {
    pub video_id: u32,
    pub category_id: u8,
    pub category: String,
    pub video_length: u8,
    pub upload_date: DateTime<Utc>,
}

impl Dummy<Faker> for Video {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &Faker, rng: &mut R) -> Self {
        let video_id = Fake::fake_with_rng(&(10000..=99999), rng);
        let categories_dict: HashMap<&str, u8> = [
            ("Education", 1),
            ("Entertainment", 2),
            ("Lifestyle", 3),
            ("Music", 4),
            ("News", 5),
            ("Sports", 6),
            ("Technology", 7),
            ("Dance", 8),
            ("Cooking", 9),
            ("Comedy", 10),
            ("Travel", 11),
        ]
        .iter()
        .cloned()
        .collect();

        let category_keys: Vec<&str> = categories_dict.keys().copied().collect();
        let category = category_keys.choose(rng).unwrap().to_string();
        let category_id = categories_dict[category.as_str()];

        let video_length = Fake::fake_with_rng(&(10..=250), rng);
        let upload_date = Utc::now();

        Video {
            video_id,
            category_id,
            category,
            video_length,
            upload_date,
        }
    }
}

impl Video {
    pub fn dummy_historical() -> Self {
        let days_ago = rand::thread_rng().gen_range(0..730);
        let mut video = Video::dummy(&Faker);
        video.upload_date = video.upload_date - Days::new(days_ago);

        video
    }

    pub fn generate(num: u32, historical: bool) -> Vec<Video> {
        let mut videos = Vec::with_capacity(num as usize);

        for _ in 0..num {
            let video = if historical {
                Video::dummy_historical()
            } else {
                Video::dummy(&Faker)
            };

            videos.push(video);
        }

        videos
    }
}
