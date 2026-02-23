
use uuid::Uuid;
use std::rc::Rc;
use std::cell::RefCell;



enum Node {
    Leaf(LeafNode),
    Internal(InternalNode),
}


struct LeafNode{
    keys: Vec<Uuid>,
    values: Vec<Vec<u8>>,
    next: Option<Rc<RefCell<LeafNode>>>,
}

struct InternalNode{
    keys: Vec<Uuid>,
    children: Vec<Rc<RefCell<Node>>>,
}