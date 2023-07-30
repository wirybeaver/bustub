#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

Context::~Context() {
  write_set_.clear();
  read_set_.clear();
  header_page_.reset();
  root_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *header_page = page_guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  auto head_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *head_page = head_page_guard.template As<BPlusTreeHeaderPage>();
  if (head_page->root_page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  auto cur_page_id = head_page->root_page_id_;
  auto cur_page_guard = bpm_->FetchPageRead(cur_page_id);
  auto *cur_page = cur_page_guard.template As<InternalPage>();
  ctx.read_set_.push_back(std::move(cur_page_guard));
  head_page_guard.Drop();
  while (!cur_page->IsLeafPage()) {
    int index = cur_page->Lookup(key, comparator_);
    cur_page_id = cur_page->ValueAt(index);
    cur_page_guard = bpm_->FetchPageRead(cur_page_id);
    cur_page = cur_page_guard.template As<InternalPage>();
    assert(ctx.read_set_.size() == 1);
    ctx.read_set_.push_back(std::move(cur_page_guard));
    ctx.read_set_.pop_front();
  }
  auto leaf_page_guard = std::move(ctx.read_set_.back());
  ctx.read_set_.pop_back();
  auto *leaf_page = leaf_page_guard.As<LeafPage>();
  auto [index, equal] = leaf_page->Lookup(key, comparator_);
  if (equal) {
    result->push_back(leaf_page->ValueAt(index));
  }
  return equal;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  auto leaf_page_id = FindLeafToModify(key, ctx, ModificationType::INSERT,
                                       [](BPlusTreePage *page) { return page->GetSize() + 1 < page->GetMaxSize(); });
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;
  }
  assert(!ctx.write_set_.empty());
  auto leaf_page_guard = std::move(ctx.write_set_.back());
  ctx.write_set_.pop_back();
  auto *leaf_page = leaf_page_guard.AsMut<LeafPage>();
  auto [index, equal] = leaf_page->Lookup(key, comparator_);
  if (equal) {
    return false;
  }

  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    return true;
  }

  page_id_t new_page_id;
  bpm_->NewPageGuarded(&new_page_id);
  auto new_page_guard = bpm_->FetchPageWrite(new_page_id);
  auto *new_page = new_page_guard.AsMut<LeafPage>();
  new_page->Init(leaf_max_size_);
  leaf_page->MoveRightHalfTo(new_page);
  new_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_page_id);

  InsertToParent(leaf_page_id, new_page_id, new_page->KeyAt(0), ctx);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto *header_page = header_page_guard.template As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafToModify(const KeyType &key, Context &ctx, ModificationType ops,
                                      const std::function<bool(BPlusTreePage *)> &safe) -> page_id_t {
  auto header_page_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = header_page_guard.template AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = header_page->root_page_id_;
  ctx.header_page_ = std::move(header_page_guard);
  if (ctx.root_page_id_ == INVALID_PAGE_ID) {
    if (ops == ModificationType::DELETE) {
      return INVALID_PAGE_ID;
    }
    page_id_t root_page_id;
    bpm_->NewPageGuarded(&root_page_id);
    auto root_page_guard = bpm_->FetchPageWrite(root_page_id);
    auto *root_page = root_page_guard.AsMut<LeafPage>();
    root_page->Init(leaf_max_size_);
    SetRootPage(root_page_id, ctx);
    ctx.write_set_.push_back(std::move(root_page_guard));
    return root_page_id;
  }
  auto cur_page_guard = bpm_->FetchPageWrite(ctx.root_page_id_);
  auto *cur_page = cur_page_guard.AsMut<InternalPage>();
  auto cur_page_id = ctx.root_page_id_;
  ctx.write_set_.push_back(std::move(cur_page_guard));
  while (!cur_page->IsLeafPage()) {
    int i = cur_page->Lookup(key, comparator_);
    assert(i >= 0 && i < cur_page->GetSize());
    cur_page_id = cur_page->ValueAt(i);
    cur_page_guard = bpm_->FetchPageWrite(cur_page_id);
    cur_page = cur_page_guard.AsMut<InternalPage>();
    ctx.write_set_.push_back(std::move(cur_page_guard));
    if (safe(cur_page)) {
      ctx.header_page_.reset();
      for (size_t k = 0; k < ctx.write_set_.size() - 1; k++) {
        ctx.write_set_.pop_front();
      }
    }
  }
  return cur_page_id;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToParent(page_id_t left_page_id, page_id_t right_page_id, const KeyType &key, Context &ctx) {
  if (ctx.IsRootPage(left_page_id)) {
    page_id_t new_root_page_id;
    bpm_->NewPageGuarded(&new_root_page_id);
    auto new_root_page_guard = bpm_->FetchPageWrite(new_root_page_id);
    auto *new_root_page = new_root_page_guard.AsMut<InternalPage>();
    new_root_page->Init(internal_max_size_);
    new_root_page->InsertFirstValue(left_page_id);
    new_root_page->Insert(key, right_page_id, comparator_);
    SetRootPage(new_root_page_id, ctx);
    return;
  }

  auto parent_page_guard = std::move(ctx.write_set_.back());
  auto parent_page_id = parent_page_guard.PageId();
  ctx.write_set_.pop_back();
  auto *parent_page = parent_page_guard.template AsMut<InternalPage>();
  if (parent_page->GetSize() < parent_page->GetMaxSize()) {
    parent_page->Insert(key, right_page_id, comparator_);
    return;
  }

  int insert_pos = parent_page->Lookup(key, comparator_) + 1;
  int size = parent_page->GetSize();
  int mid_pos = size / 2;
  assert(insert_pos == size || insert_pos < size && comparator_(parent_page->KeyAt(insert_pos), key) != 0);

  page_id_t new_parent_page_id;
  bpm_->NewPageGuarded(&new_parent_page_id);
  auto new_parent_page_guard = bpm_->FetchPageWrite(new_parent_page_id);
  auto *new_parent_page = new_parent_page_guard.template AsMut<InternalPage>();
  new_parent_page->Init(internal_max_size_);
  // [0, mid_pos) in the left node, [mid_pos, size) in the right node
  parent_page->MoveRightToHalf(new_parent_page);

  // always ensure right node size - left node size = 0 or 1
  if (insert_pos >= mid_pos) {
    if (size % 2 == 0) {
      new_parent_page->InsertAt(insert_pos - mid_pos, key, right_page_id);
    } else {
      if (insert_pos == mid_pos) {
        assert(insert_pos == parent_page->GetSize());
        parent_page->InsertAt(insert_pos, key, right_page_id);
      } else {
        new_parent_page->MoveFirstToLastOf(parent_page);
        new_parent_page->InsertAt(insert_pos - mid_pos - 1, key, right_page_id);
      }
    }
  } else {
    parent_page->InsertAt(insert_pos, key, right_page_id);
    if (size % 2 == 0) {
      parent_page->MoveLastToFirstOf(new_parent_page);
    }
  }
  auto mid_key = new_parent_page->KeyAt(0);
  new_parent_page->SetKeyAt(0, KeyType{});
  InsertToParent(parent_page_id, new_parent_page_id, mid_key, ctx);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPage(page_id_t root_page_id, Context &ctx) {
  auto guard = std::move(ctx.header_page_);
  assert(guard.has_value());
  auto *header_page = guard->AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = root_page_id;
  ctx.root_page_id_ = root_page_id;
  ctx.header_page_ = std::move(guard);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
