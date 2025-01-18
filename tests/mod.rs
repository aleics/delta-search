#[cfg(test)]
mod integration_tests {
    use lazy_static::lazy_static;
    use reqwest::{Client, StatusCode};

    lazy_static! {
        static ref CLIENT: Client = Client::new();
    }

    #[tokio::test]
    async fn test_simple() {
        // given
        let entity_name = "players";

        // creates entity
        create_entity(entity_name).await;

        // populates data
        populate_data(entity_name).await;

        // creates index
        create_index(entity_name).await;

        // gets options
        reads_filter_options(entity_name).await;

        // executes query
        executes_query(entity_name).await;

        // adds deltas
        adds_deltas(entity_name).await;
    }

    async fn create_entity(name: &str) {
        // given
        let payload = r#"{}"#;

        // when
        let response = CLIENT
            .post(format!("http://127.0.0.1:3000/entities/{}", name))
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
            .unwrap();

        // then
        assert_eq!(response.status(), StatusCode::OK)
    }

    async fn populate_data(name: &str) {
        // given
        let payload = r#"{
            "data": [
                {
                    "id": 0,
                    "fields": {
                        "name": "Michael Jordan",
                        "sport": "Basketball",
                        "birth_date": "1963-02-17",
                        "active": false,
                        "score": 9
                    }
                },
                {
                    "id": 1,
                    "fields": {
                        "name": "Lionel Messi",
                        "sport": "Football",
                        "birth_date": "1987-06-24",
                        "active": true,
                        "score": 9.5
                    }
                },
                {
                    "id": 2,
                    "fields": {
                        "name": "Cristiano Ronaldo",
                        "sport": "Football",
                        "birth_date": "1985-02-05",
                        "active": true,
                        "score": 8.7
                    }
                }
            ]
        }"#;

        // when
        let response = CLIENT
            .put(format!("http://127.0.0.1:3000/data/{}", name))
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
            .unwrap();

        // then
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn create_index(name: &str) {
        // given
        let payload = r#"{
            "name": "score",
            "type": "numeric"
        }"#;

        // when
        let response = CLIENT
            .put(format!("http://127.0.0.1:3000/indices/{}", name))
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
            .unwrap();

        // then
        assert_eq!(response.status(), StatusCode::OK);
    }

    async fn reads_filter_options(name: &str) {
        // when
        let response = CLIENT
            .get(format!("http://127.0.0.1:3000/indices/{}/options", name))
            .send()
            .await
            .unwrap();

        // then
        assert_eq!(response.status(), StatusCode::OK);

        let response_body = response.text().await.unwrap();
        assert_eq!(
            normalize(&response_body),
            normalize(
                r#"[
                    {
                        "field": "score",
                        "values": {
                            "8.7": 1,
                            "9": 1,
                            "9.5": 1
                        }
                    }
                ]"#
            )
        );
    }

    async fn executes_query(name: &str) {
        // given
        let payload = r#"{
            "filter": "score > 2",
            "sort": {
                "by": "score",
                "direction": "desc"
            },
            "page": {
                "start": 0,
                "size": 10
            }
        }"#;

        // when
        let response = CLIENT
            .post(format!("http://127.0.0.1:3000/indices/{}/search", name))
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
            .unwrap();

        // then
        assert_eq!(response.status(), StatusCode::OK);

        let response_body = response.text().await.unwrap();
        assert_eq!(
            normalize(&response_body),
            normalize(
                r#"{
                    "data": [
                        {
                            "id": 1,
                            "fields": {
                                "active": true,
                                "birth_date": "1987-06-24",
                                "name": "Lionel Messi",
                                "score": 9.5,
                                "sport": "Football"
                            }
                        },
                        {
                            "id": 0,
                            "fields": {
                                "active": false,
                                "birth_date": "1963-02-17",
                                "name": "Michael Jordan",
                                "score": 9,
                                "sport": "Basketball"
                            }
                        },
                        {
                            "id": 2,
                            "fields": {
                                "active": true,
                                "birth_date": "1985-02-05",
                                "name": "Cristiano Ronaldo",
                                "score": 8.7,
                                "sport": "Football"
                            }
                        }
                    ]
                }"#
            )
        );
    }

    async fn adds_deltas(name: &str) {
        // given
        let payload = r#"{
            "scope": {
                "context": 0,
                "date": "2020-01-01"
            },
            "deltas": [
                {
                    "id": 1,
                    "fieldName": "score",
                    "before": 9.5,
                    "after": 6
                }
            ]
        }"#;

        // when
        let response = CLIENT
            .post(format!("http://127.0.0.1:3000/deltas/{}", name))
            .header("Content-Type", "application/json")
            .body(payload)
            .send()
            .await
            .unwrap();

        // then
        assert_eq!(response.status(), StatusCode::OK);
    }

    fn normalize(input: &str) -> String {
        let mut string = input.to_string();
        string.retain(|c| !c.is_whitespace());

        string
    }
}
