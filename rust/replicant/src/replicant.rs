use crate::consensus::traits::Consensus;

pub struct Replicant {
    paxos: Box<dyn Consensus>
}

impl Replicant {
    pub fn new(paxos: Box<dyn Consensus>) -> Replicant {
        Replicant { paxos }
    }
}
