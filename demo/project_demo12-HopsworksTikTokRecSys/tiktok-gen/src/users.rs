use chrono::{DateTime, Days, Utc};
use fake::{faker::address::en::CountryName, Dummy, Fake, Faker};
use rand::seq::SliceRandom;
use rand::Rng;

#[derive(Debug)]
pub struct User {
    pub user_id: u32,
    pub gender: String,
    pub age: u16,
    pub country: String,
    pub registration_date: DateTime<Utc>,
}

impl Dummy<Faker> for User {
    fn dummy_with_rng<R: Rng + ?Sized>(_: &Faker, rng: &mut R) -> Self {
        let user_id = Fake::fake_with_rng(&(10000..=99999), rng);
        let gender = ["Male", "Female", "Others"]
            .choose(rng)
            .unwrap()
            .to_string();

        let age = Fake::fake_with_rng(&(12..=90), rng);
        let country = Fake::fake_with_rng(&(CountryName()), rng);
        let registration_date = Utc::now();

        User {
            user_id,
            gender,
            age,
            country,
            registration_date,
        }
    }
}

impl User {
    pub fn dummy_historical() -> Self {
        let days_ago = rand::thread_rng().gen_range(0..730);
        let mut user = User::dummy(&Faker);
        user.registration_date = user.registration_date - Days::new(days_ago);

        user
    }

    pub fn generate(num: u32, historical: bool) -> Vec<User> {
        let mut users = Vec::with_capacity(num as usize);

        for _ in 0..num {
            let user = if historical {
                User::dummy_historical()
            } else {
                User::dummy(&Faker)
            };

            users.push(user);
        }

        users
    }
}
