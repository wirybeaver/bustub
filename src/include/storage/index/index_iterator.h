//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t page_id, int index, std::optional<ReadPageGuard> read_page_guard,
                BufferPoolManager *buffer_pool_manager);
  IndexIterator(IndexIterator &&that) noexcept;
  auto operator=(IndexIterator &&that) noexcept -> IndexIterator &;

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool { return page_id_ == itr.page_id_ && index_ == itr.index_; }

  auto operator!=(const IndexIterator &itr) const -> bool { return page_id_ != itr.page_id_ || index_ != itr.index_; }

 private:
  // add your own private member variables here
  page_id_t page_id_{INVALID_PAGE_ID};
  int index_{-1};
  std::optional<ReadPageGuard> page_guard_{std::nullopt};
  BufferPoolManager *bpm_;
  const LeafPage *page_;
};

}  // namespace bustub
