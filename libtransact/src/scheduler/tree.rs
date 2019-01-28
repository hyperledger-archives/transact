use std;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::error::Error as StdError;
use std::rc::Rc;

#[derive(Debug)]
pub enum RadixTreeError {
    AddressNotInTree(String),
}

impl StdError for RadixTreeError {
    fn description(&self) -> &str {
        match *self {
            RadixTreeError::AddressNotInTree(ref msg) => msg,
        }
    }
}

impl std::fmt::Display for RadixTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            RadixTreeError::AddressNotInTree(ref s) => write!(f, "AddressNotInTree: {}", s),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Node<T> {
    address: String,
    children: BTreeMap<String, Rc<RefCell<Node<T>>>>,
    data: Option<T>,
}

#[derive(Default, Debug, Clone)]
pub struct RadixTree<T> {
    root: Rc<RefCell<Node<T>>>,
}

/// This radix tree is a prefix tree: a node's address is always a strict prefix of the addresses
/// of its children, and every node either has data or has multiple children.
impl<T: Clone> RadixTree<T> {
    pub fn new() -> Self {
        RadixTree {
            root: Rc::new(RefCell::new(Node {
                address: "".to_string(),
                children: BTreeMap::new(),
                data: None,
            })),
        }
    }

    fn get_child(
        node: &Rc<RefCell<Node<T>>>,
        address: &str,
    ) -> Result<Rc<RefCell<Node<T>>>, RadixTreeError> {
        node.borrow()
            .children
            .values()
            .find(|child| address.starts_with(&child.borrow().address))
            .map(|child| Rc::clone(child))
            .ok_or_else(|| RadixTreeError::AddressNotInTree("Address Not In Tree".to_string()))
    }

    /// Collects all children below ADDRESS, as well as the children's descendants
    fn walk_to_address(&self, address: &str) -> Vec<Rc<RefCell<Node<T>>>> {
        let mut current_node = Rc::clone(&self.root);
        let mut results = vec![];
        results.push(Rc::clone(&current_node));

        // A node's address is always a proper prefix of the addresses of its children
        while address != current_node.borrow().address.as_str()
            && address.starts_with(current_node.borrow().address.as_str())
        {
            match RadixTree::get_child(&current_node, address) {
                Ok(child) => {
                    results.push(Rc::clone(&child));
                    current_node = child;
                }
                Err(_) => break,
            }
        }
        results
    }

    /// Return a vector of tuple pairs of node addresses and data
    /// The Node address is the key, the data is the value.
    /// First the ancestors of ADDRESS (including self) are yielded, earliest to latest, and
    /// then the descendants of ADDRESS are yielded
    pub fn walk(&self, address: &str) -> Vec<(String, Option<T>)> {
        let mut return_nodes = Vec::new();
        let accumulated_nodes = self.walk_to_address(&address);
        for node in accumulated_nodes.iter() {
            return_nodes.push((node.borrow().address.clone(), node.borrow().data.clone()));
        }

        if let Some(node) = accumulated_nodes.iter().last() {
            let mut to_process = VecDeque::new();
            let descendants = node.borrow().children.clone();
            for descendant in descendants.values() {
                to_process.push_front(Rc::clone(&descendant));
            }
            while let Some(current_child) = to_process.pop_front() {
                return_nodes.push((
                    current_child.borrow().address.clone(),
                    current_child.borrow().data.clone(),
                ));
                let additional_descendants = &current_child.borrow().children;
                for child in additional_descendants.values() {
                    to_process.push_front(Rc::clone(&child));
                }
            }
        }
        return_nodes
    }

    /// Walk as far down the tree as possible. If the desired address is reached, return that node.
    /// Otherwise, add a new one.
    fn get_or_create(&self, address: &str) -> Rc<RefCell<Node<T>>> {
        let accumulated_nodes = self.walk_to_address(&address);
        let first_ancestor = accumulated_nodes
            .iter()
            .last()
            .expect("Node ancestors not formed correctly");

        if first_ancestor.borrow().address == address {
            return Rc::clone(&first_ancestor);
        }

        // The address isn't in the tree, so a new node will need to be added
        let new_node = Rc::new(RefCell::new(Node {
            address: address.to_string(),
            children: BTreeMap::new(),
            data: None,
        }));

        // Attempt to get the next child with a matching prefix.
        let prefix_len = first_ancestor.borrow().address.len();
        let option_ancestor_child = first_ancestor
            .borrow()
            .children
            .values()
            .find(|child| {
                let child_address = &child.borrow().address;
                let child_address_prefix: String =
                    child_address.chars().skip(prefix_len).collect::<String>();
                let address_prefix: String =
                    address.chars().skip(prefix_len).take(1).collect::<String>();
                child_address_prefix.starts_with(&address_prefix)
            })
            .map(|child| Rc::clone(child));

        // Checks if the next child with a matching prefix was found, else just adds the new
        // address as a child.
        let ancestor_child = match option_ancestor_child {
            Some(child) => child,
            None => {
                first_ancestor
                    .borrow_mut()
                    .children
                    .insert(address.to_string(), Rc::clone(&new_node));
                return new_node;
            }
        };

        // If node address is 'rustic' and the address being added is 'rust',
        // then 'rust' will be the intermediate node taking 'rustic' as a child.
        if ancestor_child.borrow().address.starts_with(address) {
            first_ancestor
                .borrow_mut()
                .children
                .insert(address.to_string(), Rc::clone(&new_node));

            new_node
                .borrow_mut()
                .children
                .insert(address.to_string(), Rc::clone(&ancestor_child));
            first_ancestor
                .borrow_mut()
                .children
                .remove(&ancestor_child.borrow().address);
            return new_node;
        };

        // The address and the match address share a common prefix, so
        // an intermediate node with the prefix as its address will
        // take them both as children.
        let ancestor_child_address = ancestor_child.borrow().address.clone();
        let shorter = if address.len() < ancestor_child_address.len() {
            address
        } else {
            &ancestor_child_address
        };
        let intermediate_node = Rc::new(RefCell::new(Node {
            address: String::from(""),
            children: BTreeMap::new(),
            data: None,
        }));

        for i in 0..shorter.len() {
            if address.chars().nth(i) != ancestor_child_address.chars().nth(i) {
                intermediate_node.borrow_mut().address = shorter[..i].to_string();
                break;
            }
        }
        let mut new_children_map = BTreeMap::new();
        new_children_map.insert(new_node.borrow().address.clone(), Rc::clone(&new_node));
        new_children_map.insert(
            ancestor_child.borrow().address.clone(),
            Rc::clone(&ancestor_child),
        );
        intermediate_node
            .borrow_mut()
            .children
            .append(&mut new_children_map);
        first_ancestor.borrow_mut().children.insert(
            intermediate_node.borrow().address.clone(),
            Rc::clone(&intermediate_node),
        );
        first_ancestor
            .borrow_mut()
            .children
            .remove(&ancestor_child.borrow().address.clone());

        new_node
    }

    /// Walk to ADDRESS, creating nodes if necessary, and set the data there to
    /// UPDATER(data)
    pub fn update(&self, address: &str, updater: &Fn(Option<T>) -> Option<T>, prune: bool) {
        let node = self.get_or_create(&address);
        let existing_data = node.borrow_mut().data.take();
        node.borrow_mut().data = updater(existing_data);

        if prune {
            node.borrow_mut().children.clear();
        }
    }

    /// Remove all children (and descendants) below ADDRESS
    pub fn prune(&self, address: &str) {
        let accumulated_nodes = self.walk_to_address(&address);
        if let Some(node) = accumulated_nodes.iter().last() {
            node.borrow_mut().children.clear()
        }
    }
}
