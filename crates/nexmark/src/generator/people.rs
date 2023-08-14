//! Generates people for the Nexmark streaming data source.
//!
//! API based on the equivalent [Nexmark Flink PersonGenerator API](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/generator/model/PersonGenerator.java).

use super::{
    super::{config as nexmark_config, model::Person},
    config, NexmarkGenerator,
};
use dbsp::{algebra::ArcStr, arcstr_literal};
use rand::{seq::SliceRandom, Rng};
use std::{
    cmp::min,
    mem::{size_of, size_of_val},
};

// Keep the number of states small so that the example queries will find
// results even with a small batch of events.
static US_STATES: [ArcStr; 6] = [
    arcstr_literal!("AZ"),
    arcstr_literal!("CA"),
    arcstr_literal!("ID"),
    arcstr_literal!("OR"),
    arcstr_literal!("WA"),
    arcstr_literal!("WY"),
];

static US_CITIES: [ArcStr; 10] = [
    arcstr_literal!("Phoenix"),
    arcstr_literal!("Los Angeles"),
    arcstr_literal!("San Francisco"),
    arcstr_literal!("Boise"),
    arcstr_literal!("Portland"),
    arcstr_literal!("Bend"),
    arcstr_literal!("Redmond"),
    arcstr_literal!("Seattle"),
    arcstr_literal!("Kent"),
    arcstr_literal!("Cheyenne"),
];

const FIRST_NAMES: &[&str] = &[
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];

const LAST_NAMES: &[&str] = &[
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];

impl<R: Rng> NexmarkGenerator<R> {
    // Generate and return a random person with next available id.
    pub fn next_person(&mut self, next_event_id: u64, timestamp: u64) -> Person {
        let id = self.last_base0_person_id(next_event_id) + config::FIRST_PERSON_ID as u64;
        let name = self.next_person_name();
        let email_address = self.next_email();
        let credit_card = self.next_credit_card();
        let city = self.next_us_city();
        let state = self.next_us_state();
        // Not sure why original doesn't include the date_time timestamp, but following
        // the same calculation.
        let current_size = size_of::<u64>()
            + size_of_val(name.as_str())
            + size_of_val(email_address.as_str())
            + size_of_val(credit_card.as_str())
            + size_of_val(city.as_str())
            + size_of_val(state.as_str());
        Person {
            id,
            name,
            email_address,
            credit_card,
            city,
            state,
            date_time: timestamp,
            extra: self.next_extra(
                current_size,
                self.config.nexmark_config.avg_person_byte_size,
            ),
        }
    }

    /// Return a random person id (base 0).
    ///
    /// Choose a random person from any of the 'active' people, plus a few
    /// 'leads'. By limiting to 'active' we ensure the density of bids or
    /// auctions per person does not decrease over time for long running
    /// jobs.  By choosing a person id ahead of the last valid person id we
    /// will make newPerson and newAuction events appear to have been
    /// swapped in time.
    ///
    /// NOTE: The above is the original comment from the Java implementation.
    /// The "base 0" is referring to the fact that the returned Id is not
    /// including the FIRST_PERSON_ID offset, and should really be "offset
    /// 0".
    pub fn next_base0_person_id(&mut self, event_id: u64) -> u64 {
        let num_people = self.last_base0_person_id(event_id) + 1;
        let active_people = min(
            num_people,
            self.config.nexmark_config.num_active_people as u64,
        );
        let n = self
            .rng
            .gen_range(0..(active_people + nexmark_config::PERSON_ID_LEAD as u64));
        num_people - active_people + n
    }

    /// Return the last valid person id (ignoring FIRST_PERSON_ID). Will be the
    /// current person id if due to generate a person.
    pub fn last_base0_person_id(&self, event_id: u64) -> u64 {
        let epoch = event_id / self.config.nexmark_config.total_proportion() as u64;
        let mut offset = event_id % self.config.nexmark_config.total_proportion() as u64;

        if offset >= self.config.nexmark_config.person_proportion as u64 {
            // About to generate an auction or bid.
            // Go back to the last person generated in this epoch.
            offset = self.config.nexmark_config.person_proportion as u64 - 1;
        }
        // About to generate a person.
        epoch * self.config.nexmark_config.person_proportion as u64 + offset
    }

    // Return a random US state.
    fn next_us_state(&mut self) -> ArcStr {
        US_STATES.choose(&mut self.rng).unwrap().clone()
    }

    // Return a random US city.
    fn next_us_city(&mut self) -> ArcStr {
        US_CITIES.choose(&mut self.rng).unwrap().clone()
    }

    // Return a random person name.
    fn next_person_name(&mut self) -> ArcStr {
        format!(
            "{} {}",
            FIRST_NAMES.choose(&mut self.rng).unwrap(),
            LAST_NAMES.choose(&mut self.rng).unwrap()
        )
        .into()
    }

    // Return a random email address.
    fn next_email(&mut self) -> ArcStr {
        format!("{}@{}.com", self.next_string(7), self.next_string(5)).into()
    }

    // Return a random credit card number.
    fn next_credit_card(&mut self) -> ArcStr {
        format!(
            "{:04} {:04} {:04} {:04}",
            &mut self.rng.gen_range(0..10_000),
            &mut self.rng.gen_range(0..10_000),
            &mut self.rng.gen_range(0..10_000),
            &mut self.rng.gen_range(0..10_000)
        )
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::make_test_generator;
    use super::*;

    #[test]
    fn test_next_person() {
        let mut ng = make_test_generator();

        let p = ng.next_person(105, 1_000_000_000_000);

        assert_eq!(
            p,
            Person {
                id: 1002,
                name: "Peter Shultz".to_string().into(),
                email_address: "AAA@AAA.com".to_string().into(),
                credit_card: "0000 0000 0000 0000".to_string().into(),
                city: "Phoenix".to_string().into(),
                state: "AZ".to_string().into(),
                date_time: 1_000_000_000_000,
                // Difference of 200 - 59 = 141, delta of 28 (141*0.2),
                // so extra 113 chars (59 + 113 = 172 = 200 - delta)
                extra: (0..113).map(|_| "A").collect::<String>().into(),
            }
        );
    }

    #[test]
    fn test_next_base0_person_id() {
        let mut ng = make_test_generator();

        // When one more than the last person id is less than the configured
        // active people (1000), the id returned is a random id from one of
        // the currently active people plus the 'lead' people.
        // Note: the mock rng is always returning zero for the random addition
        // in the range (0..active_people).
        assert_eq!(ng.next_base0_person_id(50 * 998), 0);

        // Even when one more than the last person id is equal to the configured
        // active people, the id returned is a random id from one of the
        // active people plus the 'lead' people.
        assert_eq!(ng.next_base0_person_id(50 * 999), 0);

        // When one more than the last person id is one greater than the
        // configured active people, we consider the most recent
        // NUM_ACTIVE_PEOPLE to be the active ones, and return a random id from
        // those plus the 'lead'people.
        assert_eq!(ng.next_base0_person_id(50 * 1000), 1);

        // When one more than the last person id is 501 greater than the
        // configured active people, we consider the most recent
        // NUM_ACTIVE_PEOPLE to be the active ones, and return a random id from
        // those plus the 'lead' people.
        assert_eq!(ng.next_base0_person_id(50 * 1500), 501);
    }

    #[test]
    fn test_last_base0_person_id_default() {
        let ng = make_test_generator();

        // With the default config, the first 50 events will only include one
        // person
        assert_eq!(ng.last_base0_person_id(25), 0);

        // The 50th event will correspond to the next...
        assert_eq!(ng.last_base0_person_id(50), 1);
        assert_eq!(ng.last_base0_person_id(75), 1);

        // And so on...
        assert_eq!(ng.last_base0_person_id(100), 2);
    }

    #[test]
    fn test_last_base0_person_id_custom() {
        // Set the configured bid proportion to 21,
        // which together with the other defaults for person and auction
        // proportion, makes the total 25.
        let mut ng = make_test_generator();
        ng.config.nexmark_config.bid_proportion = 21;

        // With the total proportion at 25, there will be a new person
        // at every 25th event.
        assert_eq!(ng.last_base0_person_id(25), 1);
        assert_eq!(ng.last_base0_person_id(50), 2);
        assert_eq!(ng.last_base0_person_id(75), 3);
        assert_eq!(ng.last_base0_person_id(100), 4);
    }

    #[test]
    fn test_next_us_state() {
        let mut ng = make_test_generator();

        let s = ng.next_us_state();

        assert_eq!(s, "AZ");
    }

    #[test]
    fn test_next_us_city() {
        let mut ng = make_test_generator();

        let c = ng.next_us_city();

        assert_eq!(c, "Phoenix");
    }

    #[test]
    fn test_next_person_name() {
        let mut ng = make_test_generator();

        let n = ng.next_person_name();

        assert_eq!(n, "Peter Shultz");
    }

    #[test]
    fn test_next_email() {
        let mut ng = make_test_generator();

        let e = ng.next_email();

        assert_eq!(e, "AAA@AAA.com");
    }

    #[test]
    fn test_next_credit_card() {
        let mut ng = make_test_generator();

        let e = ng.next_credit_card();

        assert_eq!(e, "0000 0000 0000 0000");
    }
}
