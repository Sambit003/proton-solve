#pragma once

#include <Common/HashTable/StringHashTable.h>

#include <numeric>

template <typename SubMaps, typename ImplTable = StringHashTable<SubMaps>, size_t BITS_FOR_BUCKET = 8>
class TwoLevelStringHashTable : private boost::noncopyable
{
protected:
    using HashValue = size_t;
    using Self = TwoLevelStringHashTable;

public:
    using Key = StringRef;
    using Impl = ImplTable;

    static constexpr UInt32 NUM_BUCKETS = 1ULL << BITS_FOR_BUCKET;
    static constexpr UInt32 MAX_BUCKET = NUM_BUCKETS - 1;

    // TODO: currently hashing contains redundant computations when doing distributed or external aggregations
    size_t hash(const Key & x) const
    {
        return const_cast<Self &>(*this).dispatch(*this, x, [&](const auto &, const auto &, size_t hash) { return hash; });
    }

    size_t operator()(const Key & x) const { return hash(x); }

    /// NOTE Bad for hash tables with more than 2^32 cells.
    static size_t getBucketFromHash(size_t hash_value) { return (hash_value >> (32 - BITS_FOR_BUCKET)) & MAX_BUCKET; }

public:
    using key_type = typename Impl::key_type;
    using mapped_type = typename Impl::mapped_type;
    using value_type = typename Impl::value_type;
    using cell_type = typename Impl::cell_type;

    using LookupResult = typename Impl::LookupResult;
    using ConstLookupResult = typename Impl::ConstLookupResult;

    Impl impls[NUM_BUCKETS];
    bool bucket_updated_flags[NUM_BUCKETS] = {false};

    TwoLevelStringHashTable() {}

    template <typename Source>
    TwoLevelStringHashTable(const Source & src)
    {
        if (src.m0.hasZero())
            impls[0].m0.setHasZero(*src.m0.zeroValue());

        for (auto & v : src.m1)
        {
            size_t hash_value = v.getHash(src.m1);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m1.insertUniqueNonZero(&v, hash_value);
            bucket_updated_flags[buck] = true;
        }
        for (auto & v : src.m2)
        {
            size_t hash_value = v.getHash(src.m2);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m2.insertUniqueNonZero(&v, hash_value);
            bucket_updated_flags[buck] = true;
        }
        for (auto & v : src.m3)
        {
            size_t hash_value = v.getHash(src.m3);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].m3.insertUniqueNonZero(&v, hash_value);
            bucket_updated_flags[buck] = true;
        }
        for (auto & v : src.ms)
        {
            size_t hash_value = v.getHash(src.ms);
            size_t buck = getBucketFromHash(hash_value);
            impls[buck].ms.insertUniqueNonZero(&v, hash_value);
            bucket_updated_flags[buck] = true;
        }
    }

    // This function is mostly the same as StringHashTable::dispatch, but with
    // added bucket computation. See the comments there.
    template <typename Self, typename Func, typename KeyHolder>
    static auto ALWAYS_INLINE dispatch(Self & self, KeyHolder && key_holder, Func && func)
    {
        StringHashTableHash hash;
        StringRef x = keyHolderGetKey(key_holder);
        const size_t sz = x.size;
        if (sz == 0)
        {
            if constexpr (std::is_same_v<Func, typename Impl::EmplaceCallable>)
                self.bucket_updated_flags[0] = true;

            keyHolderDiscardKey(key_holder);
            return func(self.impls[0].m0, VoidKey{}, 0);
        }

        if (x.data[x.size - 1] == 0)
        {
            // Strings with trailing zeros are not representable as fixed-size
            // string keys. Put them to the generic table.
            auto res = hash(x);
            auto buck = getBucketFromHash(res);
            if constexpr (std::is_same_v<Func, typename Impl::EmplaceCallable>)
                self.bucket_updated_flags[buck] = true;

            return func(self.impls[buck].ms, std::forward<KeyHolder>(key_holder),
                res);
        }

        const char * p = x.data;
        // pending bits that needs to be shifted out
        const char s = (-sz & 7) * 8;
        union
        {
            StringKey8 k8;
            StringKey16 k16;
            StringKey24 k24;
            UInt64 n[3];
        };
        switch ((sz - 1) >> 3)
        {
            case 0:
            {
                // first half page
                if ((reinterpret_cast<uintptr_t>(p) & 2048) == 0)
                {
                    memcpy(&n[0], p, 8);
                    n[0] &= -1ULL >> s;
                }
                else
                {
                    const char * lp = x.data + x.size - 8;
                    memcpy(&n[0], lp, 8);
                    n[0] >>= s;
                }
                auto res = hash(k8);
                auto buck = getBucketFromHash(res);
                if constexpr (std::is_same_v<Func, typename Impl::EmplaceCallable>)
                    self.bucket_updated_flags[buck] = true;

                keyHolderDiscardKey(key_holder);
                return func(self.impls[buck].m1, k8, res);
            }
            case 1:
            {
                memcpy(&n[0], p, 8);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[1], lp, 8);
                n[1] >>= s;
                auto res = hash(k16);
                auto buck = getBucketFromHash(res);
                if constexpr (std::is_same_v<Func, typename Impl::EmplaceCallable>)
                    self.bucket_updated_flags[buck] = true;

                keyHolderDiscardKey(key_holder);
                return func(self.impls[buck].m2, k16, res);
            }
            case 2:
            {
                memcpy(&n[0], p, 16);
                const char * lp = x.data + x.size - 8;
                memcpy(&n[2], lp, 8);
                n[2] >>= s;
                auto res = hash(k24);
                auto buck = getBucketFromHash(res);
                if constexpr (std::is_same_v<Func, typename Impl::EmplaceCallable>)
                    self.bucket_updated_flags[buck] = true;

                keyHolderDiscardKey(key_holder);
                return func(self.impls[buck].m3, k24, res);
            }
            default:
            {
                auto res = hash(x);
                auto buck = getBucketFromHash(res);
                if constexpr (std::is_same_v<Func, typename Impl::EmplaceCallable>)
                    self.bucket_updated_flags[buck] = true;

                return func(self.impls[buck].ms, std::forward<KeyHolder>(key_holder), res);
            }
        }
    }

    template <typename KeyHolder>
    void ALWAYS_INLINE emplace(KeyHolder && key_holder, LookupResult & it, bool & inserted)
    {
        dispatch(*this, key_holder, typename Impl::EmplaceCallable{it, inserted});
    }

    LookupResult ALWAYS_INLINE find(const Key x)
    {
        return dispatch(*this, x, typename Impl::FindCallable{});
    }

    ConstLookupResult ALWAYS_INLINE find(const Key x) const
    {
        return dispatch(*this, x, typename Impl::FindCallable{});
    }

    void write(DB::WriteBuffer & wb) const
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
        {
            impls[i].write(wb);
            DB::writeBoolText(bucket_updated_flags[i], wb);
        }
    }

    void writeText(DB::WriteBuffer & wb) const
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::writeChar(',', wb);
            /// <impl,updated>
            DB::writeChar('<', wb);
            impls[i].writeText(wb);
            DB::writeChar(',', wb);
            DB::writeBoolText(bucket_updated_flags[i], wb);
            DB::writeChar('>', wb);
        }
    }

    void read(DB::ReadBuffer & rb)
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
        {
            impls[i].read(rb);
            DB::readBoolText(bucket_updated_flags[i], rb);
        }
    }

    void readText(DB::ReadBuffer & rb)
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
        {
            if (i != 0)
                DB::assertChar(',', rb);
            impls[i].readText(rb);
            /// <impl,updated>
            DB::assertChar('<', rb);
            impls[i].readText(rb);
            DB::assertChar(',', rb);
            DB::readBoolText(bucket_updated_flags[i], rb);
            DB::assertChar('>', rb);
        }
    }

    size_t size() const
    {
        size_t res = 0;
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].size();

        return res;
    }

    bool empty() const
    {
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            if (!impls[i].empty())
                return false;

        return true;
    }

    size_t getBufferSizeInBytes() const
    {
        size_t res = 0;
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].getBufferSizeInBytes();

        return res;
    }

    /// proton: starts
    size_t getBufferSizeInCells() const
    {
        size_t res = 0;
        for (UInt32 i = 0; i < NUM_BUCKETS; ++i)
            res += impls[i].getBufferSizeInCells();

        return res;
    }
    /// proton: ends

    std::vector<Int64> buckets() const
    {
        std::vector<Int64> bucket_ids(NUM_BUCKETS);
        std::iota(bucket_ids.begin(), bucket_ids.end(), 0);
        return bucket_ids;
    }

    bool isUpdatedBucket(Int64 bucket_) const
    {
        return bucket_updated_flags[bucket_];
    }

    void resetUpdated(Int64 bucket_)
    {
        bucket_updated_flags[bucket_] = false;
    }

    void writeBucketUpdatedFlags(DB::WriteBuffer & wb) const
    {
        DB::writeVarUInt(NUM_BUCKETS, wb);
        for (const auto & elem : bucket_updated_flags)
            DB::writeBoolText(elem, wb);
    }

    void readBucketUpdatedFlags(DB::ReadBuffer & rb)
    {
        size_t size = 0;
        DB::readVarUInt(size, rb);
        assert(size == NUM_BUCKETS);
        for (auto & elem : bucket_updated_flags)
            DB::readBoolText(elem, rb);
    }
};
