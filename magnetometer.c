#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netdb.h>
#include <fcntl.h>
#include "HRDL.h"
#ifdef HRDL_TEST
    #include "HRDL_test_backend.c"
#endif
#include "mqtt.h"

const char* MqttEndpoint = "localhost";
const char* MqttPort = "1883";
const char* MqttClient = "Magnetometer";
const char* MqttTopic = "Magnetometer";
typedef struct MqttPublisher
{
    struct mqtt_client client;
    int sockfd;
    uint8_t sendbuf[4096];
    uint8_t recvbuf[4096];
} MqttPublisher;
static MqttPublisher g_mqtt;

static const int LogLevel = 2;
static const bool RejectMains = true;
static const int32_t SampleInterval = 3000;
static const int32_t BufferSize = 1024;
static const int32_t BlockSize = 4; // 12 s

typedef struct DataLogger
{
    int16_t handle;
    int16_t num_channels;
    int16_t num_active_channels;
    int16_t* active_channels;
    double* voltage_scaling_factors;
} DataLogger;
static DataLogger g_logger;

#pragma(pack, 1)
typedef struct MagnetometerMessage
{
    int64_t timestamp;
    double data[4];
} __attribute__((packed)) MagnetometerMessage;
#pragma(pop)

/* noreturn */ void exit_with_message(const char* message, int code)
{
    fprintf(stderr, "%s", message);
    fflush(stderr);
    exit(code);
}

int64_t current_epoch_millis()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int64_t result = (int64_t)(tv.tv_sec) * 1000 + (int64_t)(tv.tv_usec) / 1000;
    return result;
}

void published_response(void** state, struct mqtt_response_publish* publish)
{}

int open_nb_socket(const char* addr, const char* port) {
    // NOTE(cmo): From MQTT-C repo.
    // Open a non-blocking socket.
    struct addrinfo hints = {0};

    hints.ai_family = AF_UNSPEC; /* IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM; /* Must be TCP */
    int sockfd = -1;
    int rv;
    struct addrinfo *p, *servinfo;

    /* get address information */
    rv = getaddrinfo(addr, port, &hints, &servinfo);
    if(rv != 0) {
        fprintf(stderr, "Failed to open socket (getaddrinfo): %s\n", gai_strerror(rv));
        return -1;
    }

    /* open the first possible socket */
    for(p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (sockfd == -1) continue;

        /* connect to server */
        rv = connect(sockfd, p->ai_addr, p->ai_addrlen);
        if(rv == -1) {
          close(sockfd);
          sockfd = -1;
          continue;
        }
        break;
    }  

    /* free servinfo */
    freeaddrinfo(servinfo);

    /* make non-blocking */
    if (sockfd != -1) fcntl(sockfd, F_SETFL, fcntl(sockfd, F_GETFL) | O_NONBLOCK);

    /* return the new socket fd */
    return sockfd;
}

void reconnect_publisher(struct mqtt_client* c, void** reconnect_state_ptr)
{
    MqttPublisher* pub = *(MqttPublisher**)reconnect_state_ptr;
    assert(&pub->client == c);

    if (c->error != MQTT_ERROR_INITIAL_RECONNECT)
    {
        close(pub->sockfd);
        fprintf(stderr, "Reconnecting MQTT publisher. Client was in error state \"%s\"\n", 
               mqtt_error_str(c->error));
    }

    int sockfd = open_nb_socket(MqttEndpoint, MqttPort);
    if (sockfd == -1)
        exit_with_message("Failed to open socket for MQTT\n", 1);
    pub->sockfd = sockfd;

    mqtt_reinit(c, 
                sockfd,
                pub->sendbuf, sizeof(pub->sendbuf),
                pub->recvbuf, sizeof(pub->recvbuf));

    uint8_t conn_flags = MQTT_CONNECT_CLEAN_SESSION;
    mqtt_connect(c, MqttClient, NULL, NULL, 0, NULL, NULL, conn_flags, 300);
    if (c->error != MQTT_OK)
    {
        char buf[2048];
        snprintf(buf, sizeof(buf), "Failed to connect to MQTT broker (error: \"%s\")\n", mqtt_error_str(c->error));
        exit_with_message(buf, 1);
    }
}

void configure_mqtt_publisher(MqttPublisher* pub)
{
    mqtt_init_reconnect(&pub->client, reconnect_publisher, pub, published_response);
}

void close_global_mqtt_atexit()
{
    if (g_mqtt.sockfd != -1)
        close(g_mqtt.sockfd);
}


void send_mqtt_messages(MqttPublisher* pub, 
                        double* data, 
                        int32_t n_samples, 
                        int32_t n_channels, 
                        int64_t start_time)
{
    // NOTE(cmo): We're just going to encode the data as binary, no padding, 8
    // bytes of milliseconds since unix epoch, 4 x 8 bytes of doubles
    // representing the calibrated data.
    assert(sizeof(MagnetometerMessage) == (5 * 8) && 
          "Magnetometer Message struct has been padded by the compiler (or otherwise modified).");

    assert(n_channels == 4 && "Only expecting 4 channels of data");


    MagnetometerMessage msg = {};
    for (int i = 0; i < n_samples; ++i)
    {
        msg.timestamp = start_time + i * SampleInterval;
        for (int j = 0; j < n_channels; ++j)
        {
            msg.data[j] = data[i * n_channels + j];
        }

        mqtt_publish(&pub->client, MqttTopic, (void*)&msg, sizeof(msg), MQTT_PUBLISH_QOS_0);
    }

    if (pub->client.error != MQTT_OK)
    {
        char buf[2048];
        snprintf(buf, sizeof(buf), "Failed to send to MQTT message (error: \"%s\")\n", mqtt_error_str(pub->client.error));
        exit_with_message(buf, 1);
    }
}


DataLogger open_device()
{
    static int8_t description[7][25] = { "Driver Version    :",
                                         "USB Version       :",
                                         "Hardware Version  :",
                                         "Variant Info      :",
                                         "Batch and Serial  :",
                                         "Calibration Date  :",
                                         "Kernel Driver Ver.:"};
    int16_t handle = HRDLOpenUnit();

    if (handle == 0)
    {
        exit_with_message("No device found\n", 1);
    }
    else if (handle == -1)
    {
        if (LogLevel > 1)
        {
            int8_t line[80];
            HRDLGetUnitInfo(handle, line, sizeof(line), HRDL_ERROR);
            fprintf(stderr, "%s\n", line);
        }
        exit_with_message("Unable to open device\n", 1);
    }

    if (LogLevel > 1)
    {
        fprintf(stderr, "Device Information\n");
        fprintf(stderr, "==================\n");
    }

    int16_t num_channels = 0;
    int8_t line[80];
    for (int16_t l = 0; l < HRDL_ERROR; ++l)
    {
        HRDLGetUnitInfo(handle, line, sizeof(line), l);
        if (l == HRDL_VARIANT_INFO)
        {
            switch (atoi(line))
            {
                case 20:
                {
                    num_channels = 8;
                } break;
                case 24:
                {
                    num_channels = 16;
                } break;

                default:
                    exit_with_message("Unexpected device type", 1);
            }
        }

        if (LogLevel > 1)
        {
            if (l == HRDL_VARIANT_INFO)
                fprintf(stderr, "%s ADC-%s\n", description[l], line);
            else
                fprintf(stderr, "%s %s\n", description[l], line);
        }
    }

    if (LogLevel > 1)
        fprintf(stderr, "==================\n");

    DataLogger result = {
        .handle = handle,
        .num_channels = num_channels,
    };
    return result;
}

void close_device(DataLogger* d)
{
    if (d->active_channels)
    {
        free(d->active_channels);
        d->active_channels = NULL;
        d->num_active_channels = 0;
    }

    if (d->voltage_scaling_factors)
        free(d->voltage_scaling_factors);

    HRDLCloseUnit(d->handle);
    d->handle = 0;
}

void close_global_logger_atexit()
{
    close_device(&g_logger);
}

void handle_sigint(int signum)
{
    // NOTE(cmo): This is to ensure HRDLClose is called on ^C.
    if (signum == SIGINT)
        exit(0);
}

void configure_channels(DataLogger* d)
{
    // NOTE(cmo): This is very problem specific, but is easy to write equivalent
    // for other setups.
    // We enable channels 13, 14, 15, 16, with range HRDL_2500_MV (+/- 2500 mV),
    // and single_ended input.
    // Keep these in ascending order to automatically handle demuxing the stream
    // from the device.
    static const int16_t channels_to_activate[] = {13, 14, 15, 16};
    const int16_t num_active_channels = sizeof(channels_to_activate) / sizeof(channels_to_activate[0]);
    const bool single_ended = true;
    const bool activate = true;

    int16_t* active_channels = calloc(num_active_channels, sizeof(int16_t));

    for (int i = 0; i < num_active_channels; ++i)
    {
        int16_t c = channels_to_activate[i];
        int16_t status = HRDLSetAnalogInChannel(
            d->handle,
            c,
            activate,
            HRDL_2500_MV,
            single_ended
        );

        if (!status)
        {
            if (LogLevel > 1)
            {
                int8_t line[80];
                HRDLGetUnitInfo(d->handle, line, sizeof(line), HRDL_ERROR);
                fprintf(stderr, "Error: %s\n", line);
            }
            char error_buf[2048];
            snprintf(error_buf, sizeof(error_buf), "Failed to activate channel %d\n", c);
            exit_with_message(error_buf, 1);
        }

        active_channels[i] = c;
    }
    d->num_active_channels = num_active_channels;
    d->active_channels = active_channels;
}

void configure_datalogger(DataLogger* d)
{
    if (RejectMains)
    {
        bool sixty_hertz = false;
        HRDLSetMains(d->handle, (int16_t)sixty_hertz);
        if (LogLevel > 1)
            fprintf(stderr, "Setting mains noise rejection.\n");
    }

    configure_channels(d);

    // NOTE(cmo): Set sample interval.
    {
        // NOTE(cmo): We are hard-coding the 660 ms conversion time.
        // Ensure num_active_channels * 660 ms < SampleInterval

        if (SampleInterval <= d->num_active_channels * 660)
            exit_with_message("Sample interval too short to perform conversion for all channels\n", 1);

        int16_t status = HRDLSetInterval(d->handle, SampleInterval, HRDL_660MS);
        if (!status)
        {
            if (LogLevel > 1)
            {
                int8_t line[80];
                HRDLGetUnitInfo(d->handle, line, sizeof(line), HRDL_SETTINGS);
                fprintf(stderr, "Error: %s\n", line);
            }
            exit_with_message("Unable to set sampling interval.\n", 1);
        }
    }
}

void calibrate_one_reading(int32_t* data,  
                           int32_t n_channels, 
                           double* counts_conversion_factor, 
                           double* result)
{
    // NOTE(cmo): Based on Sean Leavey's code, based on Hugh Potts' code.
    assert(n_channels == 4 &&  "Calibration assumed 4 channels");

    // Scale counts to Voltage
    for (int i = 0; i < n_channels; ++i)
        result[i] = (double)data[i] * counts_conversion_factor[i];

    // resistance in the wires
    const double r_wires = 2.48;
    // input resistance
    const double r_in = 10000.0;
    // potential divider for up-down field
    const double pot_divider = 3.01 / (6.98 + 3.01);
    // nanoTesla per Volt
    const double b_scale = 1e6 / 143.0;
    // temperature sensor degrees per Volt, from LM35 10mV / deg C
    const double temp_scale = 100.0;

    // Scale the voltages to nanotesla.
    // Need to take into account the voltage drop in the wires and crosstalk between the channels
    // v_true = v_measured * (1 + r_wires / r_in) + sum(v_measured) * r_wires / r_in

    // scale z-channel to its correct value.
    result[2] /= pot_divider;

    double total_voltage = 0.0;
    for (int i = 0; i < n_channels; ++i)
        total_voltage += result[i];

    double total_voltage_corr = total_voltage * r_wires / r_in;
    for (int i = 0; i < n_channels; ++i)
    {
        result[i] *= (1.0 + r_wires / r_in);
        result[i] += total_voltage_corr;
    }

    // unit conversion
    for (int i = 0; i < 3; ++i)
        result[i] *= b_scale;
    result[3] *= temp_scale;
}

void calibrate_data(int32_t* data, 
                    int32_t n_samples, 
                    int32_t n_channels, 
                    double* counts_conversion_factor, 
                    double* result)
{
    for (int i = 0; i < n_samples; ++i)
        calibrate_one_reading(&data[i * n_channels], 
                              n_channels, 
                              counts_conversion_factor, 
                              &result[i * n_channels]
        );
}

void compute_scaling_factors(DataLogger* d)
{
    d->voltage_scaling_factors = calloc(d->num_active_channels, sizeof(double));
    for (int i = 0; i < d->num_active_channels; ++i)
    {
        int16_t c = d->active_channels[i];
        int32_t min_val, max_val;
        HRDLGetMinMaxAdcCounts(d->handle, &min_val, &max_val, c);
        // NOTE(cmo): This is hard-coded to the voltage range we use.
        d->voltage_scaling_factors[i] = 2.5 / (double)max_val; 
    }
}

void prepare_data_block(DataLogger* d)
{
    int16_t status = HRDLRun(d->handle, BlockSize, HRDL_BM_BLOCK);
    if (!status)
    {
        if (LogLevel > 1)
        {
            int8_t line[80];
            HRDLGetUnitInfo(d->handle, line, sizeof(line), HRDL_SETTINGS);
            fprintf(stderr, "Error: %s\n", line);
        }
        exit_with_message("Failed to setup data block\n", 1);
    }
}

int64_t stderr_heartbeat(int64_t prev_time)
{
    int64_t now = current_epoch_millis();

    // NOTE(cmo): 3 mins
    if (now - prev_time < 180000)
        return prev_time;

    fprintf(stderr, "Process alive at millis: %lld\n", now);

    return now;
}

int main(int argc, const char* argv[])
{
    DataLogger d = open_device();
    g_logger = d;
    atexit(close_global_logger_atexit);
    signal(SIGINT, handle_sigint);

    MqttPublisher* pub = &g_mqtt;
    configure_mqtt_publisher(pub);
    atexit(close_global_mqtt_atexit);

    configure_datalogger(&d);
    compute_scaling_factors(&d);

    int32_t data_len = BlockSize * d.num_active_channels;
    int32_t* data_block = calloc(data_len, sizeof(int32_t));
    double* calibrated_block = calloc(data_len, sizeof(double));
    int64_t prev_heartbeat_time = 0;
    while (true)
    {
        // NOTE(cmo): Start receiving a block of data.
        prepare_data_block(&d);
        int64_t block_start_timestamp = current_epoch_millis();

        // NOTE(cmo): Wait for block to fill (12s).  Do other stuff like MQTT
        // message loop in here.
        while (!HRDLReady(d.handle))
        {
            // NOTE(cmo): Sleep for only 100 ms to also pump mqtt messages.
            struct timespec sleep_time = {.tv_sec=0, .tv_nsec=100000};
            nanosleep(&sleep_time, NULL);
            mqtt_sync(&pub->client);
        }

        // NOTE(cmo): Get data from device
        int16_t overflow = 0;
        int32_t num_readings = HRDLGetValues(
            d.handle,
            data_block,
            &overflow,
            BlockSize
        );
        assert(num_readings == BlockSize && "Expected BlockSize (4) readings");
        calibrate_data(data_block, 
                       BlockSize, 
                       d.num_active_channels, 
                       d.voltage_scaling_factors, 
                       calibrated_block
        );
        send_mqtt_messages(pub, calibrated_block, BlockSize, d.num_active_channels, block_start_timestamp);
        prev_heartbeat_time = stderr_heartbeat(prev_heartbeat_time);
    }

    free(data_block);
    free(calibrated_block);
}

// http://ariel.astro.gla.ac.uk/w/bin/view/Instruments/Magnetometer
// https://github.com/picotech/picosdk-c-examples/blob/master/picohrdl/picohrdlCon/picohrdlCon.c
// https://github.com/acrerd/magnetometer/tree/master/magnetometer
// https://www.picotech.com/download/manuals/adc-20-24-data-logger-programmers-guide.pdf