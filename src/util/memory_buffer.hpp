#pragma once

#include <cstdlib>
#include <cassert>


template <typename T>
class MemoryBuffer
{
public:
	T* data_ = nullptr;
	unsigned capacity_ = 0;
	unsigned size_ = 0;
};


template <typename T>
class MemoryView
{
public:
	T* begin = nullptr;
	unsigned length = 0;
};


template <typename T>
class MemoryOffset
{
public:
	unsigned begin = 0;
	unsigned length = 0;
};


template <typename T>
class ParallelBuffer
{
public:
	T* p_data_[2] = { 0 };

	unsigned p_capacity_ = 0;
	unsigned p_size_ = 0;

	int read_id = 0;
};


namespace memory_buffer
{
	template <typename T>
	bool create_buffer(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		assert(n_elements > 0);
		assert(!buffer.data_);

		if (n_elements == 0 || buffer.data_)
		{
			return false;
		}

		buffer.data_ = (T*)std::malloc(n_elements * sizeof(T));
		assert(buffer.data_);

		if (!buffer.data_)
		{
			return false;
		}

		buffer.capacity_ = n_elements;
		buffer.size_ = 0;

		return true;
	}


	template <typename T>
	bool create_buffer(MemoryBuffer<T>& buffer, size_t n_elements)
	{
		return create_buffer(buffer, (unsigned)n_elements);
	}


	template <typename T>
	void destroy_buffer(MemoryBuffer<T>& buffer)
	{
		if (buffer.data_)
		{
			std::free(buffer.data_);
		}

		buffer.data_ = nullptr;
		buffer.capacity_ = 0;
		buffer.size_ = 0;
	}


	template <typename T>
	void reset_buffer(MemoryBuffer<T>& buffer)
	{
		buffer.size_ = 0;
	}


	template <typename T>
	void zero_buffer(MemoryBuffer<T> const& buffer)
	{
		assert(buffer.capacity_ > 0);
		assert(buffer.data_);

		if (buffer.capacity_ == 0 || !buffer.data_)
		{
			return;
		}

		using byte = unsigned char;

		constexpr auto size64 = sizeof(size_t);

		auto total_bytes = buffer.capacity_ * sizeof(T);

		auto len64 = total_bytes / size64;
		auto begin64 = (size_t*)buffer.data_;

		auto len8 = total_bytes - len64 * size64;
		auto begin8 = (byte*)(begin64 + len64);

		for (size_t i = 0; i < len64; ++i)
		{
			begin64[i] = 0;
		}

		for (size_t i = 0; i < len8; ++i)
		{
			begin8[i] = 0;
		}
	}


	template <typename T>
	T* push_elements(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		assert(n_elements > 0);

		if (n_elements == 0)
		{
			return nullptr;
		}

		assert(buffer.data_);
		assert(buffer.capacity_);

		auto is_valid =
			buffer.data_ &&
			buffer.capacity_ &&
			buffer.size_ < buffer.capacity_;

		auto elements_available = (buffer.capacity_ - buffer.size_) >= n_elements;
		assert(elements_available);

		if (!is_valid || !elements_available)
		{
			return nullptr;
		}

		auto data = buffer.data_ + buffer.size_;

		buffer.size_ += n_elements;

		return data;
	}


	template <typename T>
	T* push_elements(MemoryBuffer<T>& buffer, size_t n_elements)
	{
		return push_elements(buffer, (unsigned)n_elements);
	}


	template <typename T>
	void pop_elements(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		if (!n_elements)
		{
			return;
		}

		assert(buffer.data_);
		assert(buffer.capacity_);
		assert(n_elements <= buffer.size_);

		if (n_elements > buffer.size_)
		{
			buffer.size_ = 0;
		}
		else
		{
			buffer.size_ -= n_elements;
		}
	}


	template <typename T>
	void pop_elements(MemoryBuffer<T>& buffer, size_t n_elements)
	{
		pop_elements(buffer, (unsigned)n_elements);
	}

}


namespace memory_buffer
{
	template <typename T>
	MemoryView<T> push_view(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		assert(n_elements > 0);
		assert(buffer.data_);
		assert(buffer.capacity_);

		auto elements_available = (buffer.capacity_ - buffer.size_) >= n_elements;
		assert(elements_available);

		MemoryView<T> view{};

		view.begin = push_elements(buffer, n_elements);
		view.length = n_elements;

		return view;
	}


	template <typename T>
	MemoryView<T> push_view(MemoryBuffer<T>& buffer, size_t n_elements)
	{
		return push_view(buffer, (unsigned)n_elements);
	}


	template <typename T>
	void zero_view(MemoryView<T> const& view)
	{
		using byte = unsigned char;

		constexpr auto size64 = sizeof(size_t);

		auto len = view.length;

		auto len64 = len / size64;
		auto dst64 = (size_t*)view.begin;

		auto len8 = len - len64 * size64;
		auto dst8 = (byte*)(dst64 + len64);

		for (size_t i = 0; i < len64; ++i)
		{
			dst64[i] = 0;
		}

		for (size_t i = 0; i < len8; ++i)
		{
			dst8[i] = 0;
		}
	}


	inline MemoryView<char> push_cstr_view(MemoryBuffer<char>& buffer, unsigned total_bytes)
	{
		assert(total_bytes > 0);
		assert(buffer.data_);
		assert(buffer.capacity_);

		auto bytes_available = (buffer.capacity_ - buffer.size_) >= total_bytes;
		assert(bytes_available);

		MemoryView<char> view{};

		view.begin = push_elements(buffer, total_bytes);
		view.length = total_bytes - 1; /* zero terminated */

		return view;
	}


	inline MemoryView<char> push_cstr_view(MemoryBuffer<char>& buffer, size_t total_bytes)
	{
		return push_cstr_view(buffer, (unsigned)total_bytes);
	}


	template <typename T>
	MemoryView<T> make_view(MemoryBuffer<T> const& buffer)
	{
		assert(buffer.data_);
		assert(buffer.size_);
		assert(buffer.capacity_);

		MemoryView<T> view{};

		view.begin = buffer.data_;
		view.length = buffer.size_;

		return view;
	}


	template <typename T>
	MemoryView<T> make_view(T* data, unsigned n_elements)
	{
		assert(n_elements > 0);
		assert(data);

		MemoryView<T> view{};

		view.begin = data;
		view.length = n_elements;

		return view;
	}


	template <typename T>
	MemoryView<T> make_view(T* data, size_t n_elements)
	{
		return make_view(data, (unsigned)n_elements);
	}


	template <typename T>
	MemoryOffset<T> get_offset(MemoryView<T>& view)
	{
		MemoryOffset<T> offset{};

		offset.begin = view.begin;
		offset.length = view.length;

		return offset;
	}


	template <typename T>
	MemoryView<T> make_view(MemoryBuffer<T> const& buffer, MemoryOffset<T> const& offset)
	{
		assert(buffer.data_);
		assert(buffer.capacity_);

		assert((buffer.size_ - offset.begin) >= offset.length);

		MemoryView<T> view{};

		view.begin = buffer.data_ + offset.begin;
		view.length = offset.length;

		return view;
	}

}


namespace memory_buffer
{
	template <typename T>
	bool create_buffer(ParallelBuffer<T>& buffer, unsigned n_elements)
	{
		assert(n_elements > 0);
		assert(!buffer.p_data_[0]);

		if (n_elements == 0 || buffer.p_data_[0])
		{
			return false;
		}

		buffer.p_data_[0] = (T*)std::malloc(n_elements * sizeof(T) * 2);		
		assert(buffer.p_data_[0]);

		if (!buffer.p_data_[0])
		{
			return false;
		}

		buffer.p_data_[1] = buffer.p_data_[0] + n_elements;

		buffer.p_capacity_ = n_elements;
		buffer.p_size_ = 0;

		return true;
	}


	template <typename T>
	bool create_buffer(ParallelBuffer<T>& buffer, size_t n_elements)
	{
		return create_buffer(buffer, (unsigned)n_elements);
	}


	template <typename T>
	void zero_buffer(ParallelBuffer<T>& buffer)
	{
		assert(buffer.p_capacity_ > 0);
		assert(buffer.p_data_[0]);

		if (buffer.p_capacity_ == 0 || !buffer.p_data_[0])
		{
			return;
		}

		using byte = unsigned char;

		constexpr auto size64 = sizeof(size_t);

		auto total_bytes = buffer.p_capacity_ * sizeof(T) * 2;

		auto len64 = total_bytes / size64;
		auto begin64 = (size_t*)buffer.p_data_[0];

		auto len8 = total_bytes - len64 * size64;
		auto begin8 = (byte*)(begin64 + len64);

		for (size_t i = 0; i < len64; ++i)
		{
			begin64[i] = 0;
		}

		for (size_t i = 0; i < len8; ++i)
		{
			begin8[i] = 0;
		}
	}


	template <typename T>
	void destroy_buffer(ParallelBuffer<T>& buffer)
	{
		if (buffer.p_data_[0])
		{
			std::free(buffer.p_data_[0]);
		}		

		buffer.p_data_[0] = nullptr;
		buffer.p_data_[1] = nullptr;
		buffer.p_capacity_ = 0;
		buffer.p_size_ = 0;
	}


	template <typename T>
	void reset_buffer(ParallelBuffer<T>& buffer)
	{
		buffer.p_size_ = 0;
	}


	template <typename T>
	MemoryOffset<T> push_offset(ParallelBuffer<T>& buffer, unsigned n_elements)
	{
		assert(n_elements > 0);
		assert(buffer.p_data_[0]);
		assert(buffer.p_capacity_);

		auto is_valid =
			buffer.p_data_[0] &&
			buffer.p_capacity_ &&
			buffer.p_size_ < buffer.p_capacity_;

		auto elements_available = (buffer.p_capacity_ - buffer.p_size_) >= n_elements;
		assert(elements_available);

		if (!is_valid || !elements_available)
		{
			// error
			assert(false);
		}		

		MemoryOffset<T> offset{};
		offset.begin = buffer.p_size_;
		offset.length = n_elements;

		buffer.p_size_ += n_elements;

		return offset;
	}


	template <typename T>
	MemoryOffset<T> push_offset(ParallelBuffer<T>& buffer, size_t n_elements)
	{
		return push_offset(buffer, (unsigned)n_elements);
	}


	template <typename T>
	MemoryView<T> make_read_view(ParallelBuffer<T> const& buffer)
	{
		assert(buffer.p_data_[0]);
		assert(buffer.p_capacity_);

		MemoryView<T> view{};

		view.begin = buffer.p_data_[buffer.read_id];
		view.length = buffer.p_size_;

		return view;
	}


	template <typename T>
	MemoryView<T> make_read_view(ParallelBuffer<T> const& buffer, MemoryOffset<T> const& offset)
	{
		assert(buffer.p_data_[0]);
		assert(buffer.p_capacity_);

		assert((buffer.size_ - offset.begin) >= offset.length);

		MemoryView<T> view{};

		view.begin = buffer.p_data_[buffer.read_id] + offset.begin;
		view.length = offset.length;

		return view;
	}


	template <typename T>
	MemoryView<T> make_write_view(ParallelBuffer<T> const& buffer, MemoryOffset<T> const& offset)
	{
		assert(buffer.p_data_[0]);
		assert(buffer.p_capacity_);

		MemoryView<T> view{};

		auto write_id = (int)(!buffer.read_id);

		view.begin = buffer.p_data_[write_id] + offset.begin;
		view.length = offset.length;

		return view;
	}


	template <typename T>
	void flip_read_write(ParallelBuffer<T>& buffer)
	{
		buffer.read_id = (int)(!buffer.read_id);
	}
}