#pragma once

#include <cstdlib>
#include <cassert>


template <typename T>
class MemoryBuffer
{
public:
	T* data_ = nullptr;
	size_t capacity_ = 0;
	size_t size_ = 0;
};


template <typename T>
class MemoryView
{
public:
	T* begin = nullptr;
	size_t length = 0;
};


namespace memory_buffer
{
	template <typename T>
	bool create_buffer(MemoryBuffer<T>& buffer, size_t n_elements)
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
	void zero_buffer(MemoryBuffer<T>& buffer)
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
	T* push_elements(MemoryBuffer<T>& buffer, size_t n_elements)
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
	void pop_elements(MemoryBuffer<T>& buffer, size_t n_elements)
	{
		if (!n_elements)
		{
			return;
		}

		assert(buffer.data_);
		assert(buffer.capacity_);
		assert(n_elements <= buffer.size_);

		if(n_elements > buffer.size_)
		{
			buffer.size_ = 0;
		}
		else
		{
			buffer.size_ -= n_elements;
		}
	}


	template <typename T>
	bool create_buffer(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		return create_buffer(buffer, (size_t)n_elements);
	}


	template <typename T>
	T* push_elements(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		return push_elements(buffer, (size_t)n_elements);
	}


	template <typename T>
	void pop_elements(MemoryBuffer<T>& buffer, unsigned n_elements)
	{
		pop_elements(buffer, (size_t)n_elements);
	}


	template <typename T>
	MemoryView<T> push_view(MemoryBuffer<T>& buffer, size_t n_elements)
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
	MemoryView<T> make_view(MemoryBuffer<T>& buffer)
	{
		assert(buffer.data_);
		assert(buffer.size_);
		assert(buffer.capacity_);

		MemoryView<T> view{};

		view.begin = buffer.data_;
		view.length = buffer.size_;

		return view;
	}

}