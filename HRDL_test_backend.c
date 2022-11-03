#include "HRDL.h"
#include <stdbool.h>
#include <sys/time.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

// xoroshiro128++
/*  Written in 2019 by David Blackman and Sebastiano Vigna (vigna@acm.org)

To the extent possible under law, the author has dedicated all copyright
and related and neighboring rights to this software to the public domain
worldwide. This software is distributed without any warranty.

See <http://creativecommons.org/publicdomain/zero/1.0/>. */

#include <stdint.h>

/* This is xoshiro128++ 1.0, one of our 32-bit all-purpose, rock-solid
   generators. It has excellent speed, a state size (128 bits) that is
   large enough for mild parallelism, and it passes all tests we are aware
   of.

   For generating just single-precision (i.e., 32-bit) floating-point
   numbers, xoshiro128+ is even faster.

   The state must be seeded so that it is not everywhere zero. */


static inline uint32_t rotl(const uint32_t x, int k) {
	return (x << k) | (x >> (32 - k));
}


static uint32_t s[4] = {0xdeadbeef, 0xcafed00d, 0xfead1234, 0x12345678};
uint32_t xoro_next(void) {
	const uint32_t result = rotl(s[0] + s[3], 7) + s[0];

	const uint32_t t = s[1] << 9;

	s[2] ^= s[0];
	s[3] ^= s[1];
	s[1] ^= s[2];
	s[0] ^= s[3];

	s[2] ^= t;

	s[3] = rotl(s[3], 11);

	return result;
}
// xoroshiro128++ end

#define MaxChannels 16
typedef struct _Channel
{
    bool is_active;
} _Channel;

typedef struct _HrdlUnit
{
    bool is_open;
    bool opening_async;
    int64_t open_time;
    int16_t num_active_channels;
    int32_t sample_rate;
    int64_t prev_sample_time;
    int64_t last_run_time;
    int32_t samples_to_take;
    _Channel channels[MaxChannels+1];
} _HrdlUnit;

#define MaxUnits 16
#define MaxHandle 17
static _HrdlUnit _g_units[MaxHandle];
// NOTE(cmo): First one is never initialised since handle can't be 0/null.

int64_t _current_epoch_millis()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t result = (int64_t)(tv.tv_sec) * 1000 + (int64_t)(tv.tv_usec) / 1000;
    return result;
}

void _init_unit(_HrdlUnit* unit, bool async)
{
    unit->is_open = true;
    unit->open_time = _current_epoch_millis();
    unit->opening_async = async;
    unit->num_active_channels = 0;
}

int16_t HRDLOpenUnit()
{
    _HrdlUnit* unit;

    int i;
    for (i = 1; i < MaxHandle; ++i)
    {
        if (_g_units[i].is_open)
            continue;

        unit = &_g_units[i];
        _init_unit(unit, false);
        break;
    }

    // NOTE(cmo): No device found.
    if (i == MaxHandle)
        return 0;

    return i;
}

int16_t HRDLOpenUnitAsync()
{
    _HrdlUnit* unit;

    int i;
    for (i = 1; i < MaxHandle; ++i)
    {
        if (_g_units[i].is_open)
            continue;

        unit = &_g_units[i];
        _init_unit(unit, true);
        break;
    }

    // NOTE(cmo): No device found.
    if (i == MaxHandle)
        return 0;

    return 1;
}

int16_t HRDLOpenUnitProgress(int16_t* handle, int16_t* progress)
{
    int i;
    for (i = 1; i < MaxHandle; ++i)
    {
        if (_g_units[i].opening_async)
        {
            _g_units[i].opening_async = false;
            *handle = i;
        }
    }

    if (i == MaxHandle)
        *handle = 0;

    *progress = 100;
    return 1;
}

int16_t HRDLGetUnitInfo(int16_t handle, int8_t* string, int16_t stringLength, int16_t info)
{
    // NOTE(cmo): This code is very far from efficient *shrug*
    if (!string)
        return 0;

    if (info == HRDL_DRIVER_VERSION)
    {
        strncpy(string, "1.0.0.1", stringLength);
        return strlen(string);
    }

    if (handle > MaxHandle)
        return 0;

    if (info > HRDL_SETTINGS)
        return 0;

    if (!_g_units[handle].is_open && info != HRDL_ERROR)
        return 0;

    switch (info)
    {
        case HRDL_USB_VERSION: {
            strncpy(string, "1.1", stringLength);
        } break;

        case HRDL_HARDWARE_VERSION: {
            strncpy(string, "1", stringLength);
        } break;

        case HRDL_VARIANT_INFO: {
            strncpy(string, "24", stringLength);
        } break;

        case HRDL_BATCH_AND_SERIAL: {
            strncpy(string, "CMY02/116", stringLength);
        } break;

        case HRDL_CAL_DATE: {
            strncpy(string, "09Sep05", stringLength);
        } break;

        case HRDL_KERNEL_DRIVER_VERSION: {
            strncpy(string, "1234", stringLength);
        } break;

        case HRDL_ERROR: {
            strncpy(string, "4", stringLength);
        } break;

        case HRDL_SETTINGS: {
            strncpy(string, "9", stringLength);
        } break;
    }

    return strlen(string);
}

int16_t HRDLCloseUnit(int16_t handle)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    _g_units[handle].is_open = false;
    return 1;
}

int16_t HRDLGetMinMaxAdcCounts(int16_t handle, int32_t* minAdc, int32_t* maxAdc, int16_t channel)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    *minAdc = -INT32_MIN;
    *maxAdc = INT32_MAX;

    return 1;
}

int16_t HRDLSetAnalogInChannel(int16_t handle, int16_t channel, int16_t enabled, int16_t range, int16_t singleEnded)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    if (channel-1 < 0 || channel-1 >= MaxChannels)
        return 0;

    _HrdlUnit* unit = &_g_units[handle];
    if (!unit->is_open)
        return 0;

    if (!singleEnded)
    {
        if (channel % 1 != 0)
            return 0;

        if (((channel+1)-1) >= MaxChannels)
            return 0;

        if (unit->channels[channel+1].is_active)
            return 0;
    }
    bool prev_active = unit->channels[channel].is_active;
    unit->channels[channel].is_active = enabled;
    unit->num_active_channels += enabled - prev_active;
    return 1;
}

int16_t HRDLSetInterval(int16_t handle, int32_t sampleInterval_ms, int16_t conversionTime)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    _HrdlUnit* unit = &_g_units[handle];

    unit->sample_rate = sampleInterval_ms;
    return 1;
}

int16_t HRDLRun(int16_t handle, int32_t nValues, int16_t method)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    if (method == HRDL_BM_BLOCK)
        _g_units[handle].samples_to_take = nValues;
    else
        _g_units[handle].samples_to_take = 0;
    _g_units[handle].prev_sample_time = _current_epoch_millis();
    _g_units[handle].last_run_time = _current_epoch_millis();

    return 1;
}

int16_t HRDLReady(int16_t handle)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    int64_t now = _current_epoch_millis();

    _HrdlUnit* unit = &_g_units[handle];
    if (unit->samples_to_take > 0)
    {
        if (unit->samples_to_take * unit->sample_rate <= now - unit->prev_sample_time)
        {
            return 1;
        }
        else
        {
            return 0;
        }
    }

    if (unit->sample_rate <= now - unit->prev_sample_time)
    {
        return 1;
    }

    return 0;
}

void HRDLStop(int16_t handle)
{
}

int32_t _getnextval()
{
    return (int32_t)(xoro_next() / 2);
}

int32_t HRDLGetValues(int16_t handle, int32_t* values, int16_t* overflow, int32_t no_of_values)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    _HrdlUnit* unit = &_g_units[handle];
    *overflow = 0;

    int32_t n_req_samples = unit->samples_to_take;
    if (n_req_samples == 0)
        n_req_samples = 1;

    while (n_req_samples * unit->sample_rate > _current_epoch_millis() - unit->prev_sample_time)
        sleep(1);

    int64_t now = _current_epoch_millis();
    int64_t duration = now - unit->prev_sample_time;

    int n_samples_ready = duration / (n_req_samples * unit->sample_rate);
    if (no_of_values > n_samples_ready * unit->num_active_channels)
        no_of_values = n_samples_ready;

    if (unit->samples_to_take > 0 && unit->samples_to_take * unit->num_active_channels > no_of_values)
        no_of_values = unit->samples_to_take;

    for (int i = 0; i < no_of_values * unit->num_active_channels; ++i)
        values[i] = _getnextval();

    now = _current_epoch_millis();
    unit->prev_sample_time = now;

    return no_of_values;
}

int32_t HRDLGetTimesAndValues(int16_t handle, int32_t* times, int32_t* values, int16_t* overflow, int32_t no_of_values)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    _HrdlUnit* unit = &_g_units[handle];
    *overflow = 0;

    int32_t n_req_samples = unit->samples_to_take;
    if (n_req_samples == 0)
        n_req_samples = 1;

    while (n_req_samples * unit->sample_rate > _current_epoch_millis() - unit->prev_sample_time)
        sleep(1);

    int64_t now = _current_epoch_millis();
    int64_t duration = now - unit->prev_sample_time;

    int n_samples_ready = duration / (n_req_samples * unit->sample_rate);
    if (no_of_values > n_samples_ready * unit->num_active_channels)
        no_of_values = n_samples_ready;

    if (unit->samples_to_take > 0 && unit->samples_to_take * unit->num_active_channels < no_of_values)
        no_of_values = unit->samples_to_take;

    int time_idx = 0;
    for (int i = 0; i < no_of_values * unit->num_active_channels; ++i)
    {
        values[i] = _getnextval();
        if ((i % unit->num_active_channels) == 0)
        {
            times[i / unit->num_active_channels] = unit->prev_sample_time + time_idx * unit->sample_rate - unit->last_run_time;
            time_idx += 1;
        }
    }

    now = _current_epoch_millis();
    unit->prev_sample_time = now;
    return no_of_values;
}

// NOTE(cmo): Not implementing the SingleValue functions

int16_t HRDLSetMains(int16_t handle, int16_t sixtyHertz)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    return 1;
}

int16_t HRDLGetNumberOfEnabledChannels(int16_t handle, int16_t* nEnabled)
{
    if (handle <= 0 || handle >= MaxHandle)
        return 0;

    *nEnabled = _g_units[handle].num_active_channels;

    return 1;
}