import java.util.*;
public class RedBlackTree<K, V>{

    Node<K, V> root;
    int size;

    public static void main(String[] args){
        RedBlackTree<Integer, Integer> rbt = new RedBlackTree();
        int num = 10;
        // int[] TEST_01 = new int[]{20, 10, 5, 30, 40, 57, 3, 2, 4, 35, 25, 18, 22, 23,24, 19, 18};
        int[] TEST_01 = new int[]{};
        // for(int i = 0; i < TEST_01.length; i++){
        //     rbt.add(TEST_01[i], i);
        // }
        // rbt.printTree();
        Scanner input = new Scanner(System.in);
        // while(true){
        //     System.out.println("input a number to remove");
        //     int x = input.nextInt();
        //     rbt.get(x);
        //     rbt.remove(x);
        //     rbt.printTree();
        // }
        while(true){
            System.out.println("input a number to insert");
            int x = input.nextInt();
            rbt.add(x, x);
            rbt.printTree();
        }

    }

    class Node<K, V> {
        K key;
        V value;
        Node<K, V> left,right,parent;
        boolean isLeftChild, black;

        public Node(K key, V value){
            this.key = key;
            this.value = value;
            left = right = parent = null;
            isLeftChild = false;
            black = false;
        }

    }

    public void printTree(){
        List<List<String>> outputs = printTree(root);
        for(List<String> ss : outputs){
            for(String s : ss){
                System.out.print(s);
            }
            System.out.println();
        }
    }

    private List<List<String>> printTree(Node<K, V> node) {
        int height = dfs(node);
        int m = height, n =(int) Math.pow(2, height) - 1;
        List<List<String>> ans = new ArrayList(16);
        for(int i = 0; i < m; i++){
            List<String> item = new ArrayList();
            for(int j = 0; j < n; j++){
                item.add("  ");
            }
            ans.add(item);
        }
        Queue<Node<K,V>> q = new LinkedList();
        Queue<Integer> qi = new LinkedList();
        q.offer(node);
        qi.offer((n - 1) / 2);
        int level = 0;
        while(!q.isEmpty()){
            List<String> item = ans.get(level);
            int maxLen =(int) Math.pow(2, level);
            int size = q.size();
            for(int i = 0; i < size && i < maxLen; i++){
                Node<K,V> tn = q.poll();
                Integer index = qi.poll();

                if(!tn.black){
                    item.set(index,  "\u001b[4;31;160m"+tn.key +(tn.isLeftChild ?"L" :"R")  + "\u001b[0m" + "" );
                }else{
                    item.set(index,  tn.key+(tn.isLeftChild ?"L" :"R") + "" );
                }
                if(tn.left != null){
                    q.offer(tn.left);
                    qi.offer(index - (int) Math.pow(2, height - level - 2));
                }
                if(tn.right != null){
                    q.offer(tn.right);
                    qi.offer(index + (int) Math.pow(2, height - level - 2));
                }
            }
            level++;
        }
        return ans;
    }

    int dfs(Node<K,V> node){
        if(node == null){
            return 0;
        }
        int left = dfs(node.left);
        int right = dfs(node.right);
        return Math.max(left, right) + 1;
    }

    public V getRoot(){
        if(root == null){
            return null;
        }
        return root.value;
    }

    public V get(K key){
        Node<K, V> node = search(root, key);
        if(node == null){
            return null;
        }
        return node.value;
    }

    public void add(K key, V value){
        Node<K, V> node = new Node<K, V>(key, value);
        // System.out.println("new node, key:" + node.key +", value:" + node.value);
        if(root == null){
            root = node;
            root.black = true;
            size++;
            return ;
        }
        add(root, node);
        size++;
        return ;
    }

    private void add(Node<K, V> parent, Node<K, V> newNode){
        // the value of new node is greater than the value of parent node. insert it to right of parent node.
        // System.out.println("add check key:" + parent.key);
        if(((Comparable<K>) newNode.key).compareTo(parent.key) > 0){
            if(parent.right == null){
                // System.out.println("insert to the right of " + parent.key);
                newNode.isLeftChild = false;
                parent.right = newNode;
                newNode.parent = parent;
                checkColor(newNode);
                return;
            }else{
                add(parent.right, newNode);
                return ;
            }
        }
        // insert the new node to the left of parent.
        if(parent.left == null){
            newNode.isLeftChild = true;
            parent.left = newNode;
            newNode.parent = parent;
            checkColor(newNode);
            // System.out.println("insert to the left of " + parent.key);
            return ;
        }else{
            add(parent.left, newNode);
            return ;
        }
    }

    public void checkColor(Node<K, V> node){
        // System.out.println("check node:" + node.key);
        if(root == node){
            fixDoubleRed(root);
            return ;
        }
        // if aunt is black, then
        if(!node.black && !node.parent.black){
            fixDoubleRed(node);
        }
        checkColor(node.parent);
        return ;
    }

    public void fixDoubleRed(Node<K, V> node){
        // System.out.println("correct node:" + node.key);
        // if(node.parent != null){
        //     System.out.println("node.parent.isLeftChild?"+node.parent.isLeftChild);
        // }
        if(node == root){
            node.black = true;
            return;
        }
        if(node.parent.isLeftChild){
            // if aunt is black, rotate the tree.
            if(node.parent.parent.right == null ||
                    node.parent.parent.right.black){
                rotate(node);
                return ;
            }
            // if aunt is red, color flip.
            node.parent.parent.black = false;
            node.parent.parent.right.black = true;
            // color the cur node and silbing
            node.parent.black = true;
            node.black = false;
            return ;
        }else{
            // the parent of the node is the right child of grandparent.
            // aunt is black
            if(node.parent.parent.left == null ||
                    node.parent.parent.left.black){
                rotate(node);
                return ;
            }else{
                // if aunt is red, color flip.
                node.parent.parent.black = false;
                node.parent.parent.left.black = true;
                // color the cur node and silbing
                node.parent.black = true;
                node.black = false;
                return;
            }
        }
    }

    public void rotate(Node<K, V> node){
        if(node.isLeftChild){
            if(node.parent.isLeftChild){
                // just right rotate.
                rightRotate(node.parent.parent);
                node.black = false;
                node.parent.black = true;
                if(node.parent.right != null){
                    node.parent.right.black = false;
                }
                return;
            }else{
                // left rotate and then right rotate.
                rightLeftRotate(node.parent.parent);
                // node become the new parent.
                node.black = true;
                node.left.black = false;
                node.right.black = false;
                return ;
            }
        }else{
            if(node.parent.isLeftChild){
                // right left rotate
                leftRightRotate(node.parent.parent);
                node.black = true;
                node.left.black = false;
                node.right.black = false;
                return ;
            }else{
                // left rotate.
                leftRotate(node.parent.parent);
                node.black = false;
                node.parent.black = true;
                if(node.parent.left != null){
                    node.parent.left.black = false;
                }
                return ;
            }
        }
    }

    public void rightRotate(Node<K, V> node){
        // System.out.println("called right rotate on key:" + node.key);
        Node<K, V> temp = node.left;
        // temp right child be the left child of node.
        node.left = temp.right;
        if(temp.right != null){
            node.left = temp.right;
            node.left.parent = node;
            node.left.isLeftChild = true;
        }

        if(node == root){
            root = temp;
            temp.parent = null;
        }else{
            if(node.isLeftChild){
                node.parent.left = temp;
                temp.isLeftChild = true;
            }else{
                node.parent.right = temp;
                temp.isLeftChild = false;
            }
            temp.parent = node.parent;
        }

        node.parent = temp;
        temp.right = node;
        node.isLeftChild = false;
        return ;
    }

    public void leftRotate(Node<K, V> node){
        //    System.out.println("called left rotate on key:" + node.key);
        Node<K, V> temp = node.right;

        node.right = temp.left;
        if(temp.left != null){
            node.right = temp.left;
            node.right.parent = node;
            node.right.isLeftChild = false;
        }

        if(node == root){
            root = temp;
            temp.parent = null;
        }else{
            if(node.isLeftChild){
                node.parent.left = temp;
                temp.isLeftChild = true;
            }else{
                node.parent.right = temp;
                temp.isLeftChild = false;
            }
            temp.parent = node.parent;
        }

        node.parent = temp;
        temp.left = node;
        node.isLeftChild = true;
        return;
    }

    public void leftRightRotate(Node<K, V> node){
        leftRotate(node.left);
        rightRotate(node);
        return ;
    }

    public void rightLeftRotate(Node<K, V> node){
        rightRotate(node.right);
        leftRotate(node);
        return ;
    }

    public int height(){
        if(root == null){
            return 0;
        }
        return height(root) + 1;
    }

    public int height(Node<K, V> node){
        if(node == null){
            return 0;
        }
        int leftSubTreeHeight = height(node.left) + 1;
        int rightSubTreeHeight = height(node.right) + 1;

        return leftSubTreeHeight > rightSubTreeHeight ?
                leftSubTreeHeight :
                rightSubTreeHeight;
    }

    public int blackHeight(Node<K, V> node){
        if(node == null){
            return 1;
        }
        int leftBlackNodes = blackHeight(node.left);
        int rightBlackNodes = blackHeight(node.right);
        if(leftBlackNodes != rightBlackNodes){
            // throw a error
            // or fix the tree.
        }
        if(node.black){
            return leftBlackNodes + 1;
        }
        return leftBlackNodes;
    }

    public void remove(K key){
        if(root == null){
            return ;
        }

        System.out.println("remove:" + key);
        remove(root, key);
        return ;
    }

    private void remove(Node<K, V> node, K key){
        if(node == null){
            return ;
        }
        if(((Comparable<K>) node.key).compareTo(key) > 0){
            remove(node.left, key);
            return ;
        }
        if(((Comparable<K>) node.key).compareTo(key) < 0){
            remove(node.right, key);
            return ;
        }
        // node is inner node. find a node to replace it.
        if(node.left != null || node.right != null){
            // find a predecessor or successor as substitute node. use it replace current node. then delete the substitute node.
            Node<K, V> substitute = null;
            if(node.left != null && node.right != null){
                // find predecessor.
                substitute = findInorderSuccessor(node);
            }else if(node.left != null){
                substitute = findInorderPredecessor(node);
            }else{
                substitute = findInorderSuccessor(node);
            }
            System.out.println("remove:" + node.key + "; replace to remove :" + substitute.key);
            node.key = (K) substitute.key;
            node.value = (V) substitute.value;
            remove(substitute, substitute.key);
            return ;
        }
        // node is root, and can not find a node to replace it.
        if(node == root){
            root = null;
            size = 0;
            return ;
        }
        if(!node.black){
            // red leaf node, just simple delete.
            // here we do nothing.
        }else{
            // black leaf node, delete it will cause black height - 1 on a path.
            // we have to restore the black height
            fixDoubleBlack(node);
        }
        if(node.isLeftChild){
            node.parent.left = null;
        }else{
            node.parent.right = null;
        }
        size--;
        return ;
    }

    public void fixDoubleBlack(Node<K, V> node){
        System.out.println(" fixDoubleBlack node:" + node.key + " is black node.");
        if(node == root){
            return ;
        }
        if(!node.black){
            node.black = true;
            return ;
        }

        Node<K, V> parent = node.parent;
        Node<K, V> sibling = node.isLeftChild ? parent.right : parent.left;
        if(node.isLeftChild){
            // case 1
            if(sibling.black &&
                    (sibling.left == null || sibling.left.black) &&
                    (sibling.right == null || sibling.right.black)){
                // sibling is black and its children are both black (or null). make sibling red. then reduce the problem to fix the parent of node.
                System.out.println("case 1");
                sibling.black = false;
                fixDoubleBlack(node.parent);
                return ;
            }
            // case 4

            if(!sibling.black){
                System.out.println("case 4");
                sibling.black = parent.black;
                parent.black = false;
                leftRotate(parent);
                fixDoubleBlack(node);
            }
            // case 2
            if(sibling.right != null && !sibling.right.black){

                System.out.println("case 2");
                // sibling has a right red child.
                if(parent.black){
                    // left rotate.
                    sibling.right.black = true;
                    leftRotate(parent);
                }else{
                    // right left rotate.
                    parent.black = true;
                    rightLeftRotate(parent);
                }
                return ;

            }
            // case 3
            if(sibling.left != null && !sibling.left.black){
                System.out.println("case 3");
                // sibling has a red left child and a black right child.
                sibling.black = false;
                sibling.left.black = true;
                rightRotate(sibling);
                fixDoubleBlack(node);
                return ;
            }
        }
        // is right child.
        else{
            // case 1
            if(sibling.black &&
                    (sibling.left == null || sibling.left.black) &&
                    (sibling.right == null || sibling.right.black)){
                // sibling is black and its children are both black (or null). make sibling red. then reduce the problem to fix the parent of node.
                System.out.println("case 1");
                sibling.black = false;
                fixDoubleBlack(node.parent);
                return ;
            }
            // case 4
            if(!sibling.black){
                // sibling is red and not only its children are both black but the parent is also black.
                System.out.println("case 4");
                parent.black = false;
                sibling.right.black = true;
                rightRotate(parent);
                fixDoubleBlack(node);
            }
            // case 2
            if(sibling.left != null && !sibling.left.black){
                // sibling has a red left child.
                System.out.println("case 2");
                sibling.left.black = true;
                sibling.black = parent.black;
                parent.black = true;
                rightRotate(parent);
                return ;
            }
            // case 3
            if((sibling.left == null || sibling.left.black) &&
                    (sibling.right != null && !sibling.right.black)){
                // sibling has black left and right right. just left rotate the sibling.
                System.out.println("case 3");
                sibling.black = false;
                sibling.right.black = true;
                leftRotate(sibling);
                fixDoubleBlack(node);
                return ;
            }
        }

    }

    private Node<K, V> findInorderPredecessor(Node<K, V> ancestor){
        if(ancestor.left == null){
            return null;
        }
        Node<K, V> predecessor = ancestor.left;
        while(predecessor.right != null){
            predecessor = predecessor.right;
        }
        return predecessor;
    }

    private Node<K, V> findInorderSuccessor(Node<K, V> ancestor){
        if(ancestor.right == null){
            return null;
        }
        Node<K, V> successor = ancestor.right;
        while(successor.left != null){
            successor = successor.left;
        }
        return successor;
    }

    private Node<K, V> search(Node<K, V> node, K key){
        if(node == null){
            return null;
        }
        System.out.println("access node:" + node.key);
        int cmp = ((Comparable<K>) node.key).compareTo(key);
        if(cmp < 0){
            return search(node.right, key);
        }else if(cmp > 0){
            return search(node.left, key);
        }
        return node;
    }
}

