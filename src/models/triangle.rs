use uuid::Uuid;
use super::step::Step;

#[derive(Clone, Debug)]
pub struct Triangle {
    pub id: String,
    pub steps: [Step; 3],
}

impl Triangle {
    pub fn new(step1: Step, step2: Step, step3: Step) -> Self {
        Triangle {
            id: Uuid::new_v4().to_string(),
            steps: [step1, step2, step3],
        }
    }
}

impl Triangle {
    pub fn symbols(&self) -> Vec<String> {
        self.steps
            .iter()
            .map(|step| step.pair.clone())
            .collect()
    }
}
