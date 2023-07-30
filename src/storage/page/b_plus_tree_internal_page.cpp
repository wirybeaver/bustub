//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  if (index < 0 || index >= GetSize()) {
    return KeyType{};
  }
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= GetSize()) {
    return;
  }
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return ValueType{};
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value,
                                            const KeyComparator &keyComparator) {
  assert(GetSize() < GetMaxSize());
  int insert_pos = Lookup(key, keyComparator) + 1;
  assert(insert_pos > 0);
  InsertAt(insert_pos, key, value);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertAt(int index, const KeyType &key, const ValueType &value) {
  assert(index >= 0 && index <= GetSize() && index < GetMaxSize());
  for (int i = GetSize(); i > index; i--) {
    std::swap(array_[i], array_[i - 1]);
  }
  array_[index] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirstValue(const ValueType &value) {
  assert(GetSize() == 0);
  array_[0] = std::make_pair(KeyType{}, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveRightToHalf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  int n = GetSize();
  int mid = n / 2;
  for (int i = mid, j = 0; i < n; i++, j++) {
    std::swap(array_[i], recipient->array_[j]);
  }
  recipient->IncreaseSize(n - mid);
  IncreaseSize(-(n - mid));
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &keyComparator) const -> int {
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = (right - left) / 2 + left;
    if (keyComparator(array_[mid].first, key) <= 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return right;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToLastOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  recipient->array_[recipient->GetSize()] = EraseAt(0);
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFirstOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  MappingType tmp = EraseAt(GetSize() - 1);
  for (int i = recipient->GetSize(); i > 0; i--) {
    std::swap(recipient->array_[i], recipient->array_[i - 1]);
  }
  recipient->array_[0] = tmp;
  recipient->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllToEndOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *recipient) {
  auto i = 0;
  auto j = recipient->GetSize();
  for (; i < GetSize(); i++, j++) {
    std::swap(recipient->array_[j], array_[i]);
  }
  recipient->IncreaseSize(i);
  IncreaseSize(-i);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::EraseAt(int index) -> MappingType {
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

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
