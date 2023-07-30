//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index < 0 || index >= GetSize()) {
    return KeyType{};
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const
    -> std::pair<int, bool> {
  int left = 0;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (right - left) / 2 + left;
    if (comparator(array_[mid].first, key) >= 0) {
      right = mid - 1;
    } else {
      left = mid + 1;
    }
  }
  return std::make_pair(left, left >= GetSize() ? false : comparator(array_[left].first, key) == 0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return ValueType{};
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  auto [index, equal] = Lookup(key, comparator);
  if (equal) {
    return false;
  }
  for (int i = GetSize(); i > index; i--) {
    std::swap(array_[i], array_[i - 1]);
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  auto [index, equal] = Lookup(key, comparator);
  if (!equal) {
    return false;
  }
  for (int i = index; i < GetSize() - 1; i++) {
    std::swap(array_[i], array_[i + 1]);
  }
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveRightHalfTo(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  int mid = GetSize() / 2;
  int j = 0;
  for (int i = mid; i < GetSize(); i++, j++) {
    std::swap(array_[i], recipient->array_[j]);
  }
  IncreaseSize(-j);
  recipient->IncreaseSize(j);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToLastOf(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  recipient->array_[recipient->GetSize()] = EraseAt(0);
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFirstOf(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  MappingType tmp = EraseAt(GetSize() - 1);
  for (int i = recipient->GetSize(); i > 0; i--) {
    std::swap(recipient->array_[i], recipient->array_[i - 1]);
  }
  recipient->array_[0] = tmp;
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllToEndOf(B_PLUS_TREE_LEAF_PAGE_TYPE *recipient) {
  auto i = 0;
  auto j = recipient->GetSize();
  for (; i < GetSize(); i++, j++) {
    std::swap(recipient->array_[j], array_[i]);
  }
  recipient->IncreaseSize(i);
  IncreaseSize(-i);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::EraseAt(int index) -> MappingType {
  if (index < 0 || index >= GetSize()) {
    return std::make_pair(KeyType{}, ValueType{});
  }
  MappingType tmp = array_[index];
  for (int i = index + 1; i < GetSize(); i++) {
    std::swap(array_[i - 1], array_[i]);
  }
  IncreaseSize(-1);
  return tmp;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
