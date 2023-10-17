#pragma once
#include <chrono>

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