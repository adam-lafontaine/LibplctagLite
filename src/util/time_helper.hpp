#pragma once

#include <chrono>
#include <thread>


class Stopwatch
{
private:
	std::chrono::system_clock::time_point start_;
	std::chrono::system_clock::time_point end_;
	bool is_on_ = false;

	std::chrono::system_clock::time_point now() { return std::chrono::system_clock::now(); }

public:
	Stopwatch()
	{
		start_ = now();
		end_ = start_;
	}

	void start()
	{
		start_ = now();
		is_on_ = true;
	}

	void stop()
	{
		end_ = now();
		is_on_ = false;
	}

	double get_time_milli()
	{

		std::chrono::duration<double, std::milli> delay = is_on_ ? now() - start_ : end_ - start_;

		return delay.count();
	}

	double get_time_sec()
	{
		std::chrono::duration<double> delay = is_on_ ? now() - start_ : end_ - start_;

		return delay.count();
	}

};


namespace time_helper
{
    namespace chr = std::chrono;

	inline void delay_current_thread_ms(Stopwatch& sw, double min_delay_ms = 20.0)
	{
		auto ms = sw.get_time_milli();
		if (ms < min_delay_ms)
		{
			auto delay_ms = (unsigned long long)(min_delay_ms - ms);
			std::this_thread::sleep_for(chr::milliseconds(delay_ms));
		}
	}


	inline long long get_timestamp()
	{
		return chr::duration_cast<chr::milliseconds>(chr::system_clock::now().time_since_epoch()).count();
	}


	inline void delay_current_thread_ms(unsigned long long delay_ms = 20)
	{
		std::this_thread::sleep_for(chr::milliseconds(delay_ms));
	}
}

