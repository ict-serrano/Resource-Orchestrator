
HPC_SERVICE_NAME_FFT = "fft"
HPC_SERVICE_NAME_KMEANS = "kmean"
HPC_SERVICE_NAME_KNN = "knn"
HPC_SERVICE_NAME_KALMAN = "kalman"
HPC_SERVICE_NAME_SAVGOL = "savitzky_golay"
HPC_SERVICE_NAME_MIN_MAX = "min_max"
HPC_SERVICE_NAME_BLACK_SCHOLES = "black_scholes"
HPC_SERVICE_NAME_WAVELET = "wavelet"

class ServiceTemplate:

    def __init__(self, kernel_name, arguments):

        hpc_service_kernel_name_by_sdk_name = {"savgol": HPC_SERVICE_NAME_SAVGOL, "kalman": HPC_SERVICE_NAME_KALMAN,
                                               "fft": HPC_SERVICE_NAME_FFT, "wavelet": HPC_SERVICE_NAME_WAVELET,
                                               "black_scholes": HPC_SERVICE_NAME_BLACK_SCHOLES,
                                               "kmeans": HPC_SERVICE_NAME_KMEANS, "knn": HPC_SERVICE_NAME_KNN}

        self.__kernel_name = hpc_service_kernel_name_by_sdk_name[kernel_name]

        self.__arguments = arguments
        self.__description = None

        self.__results_filename = {HPC_SERVICE_NAME_FFT: "",
                                   HPC_SERVICE_NAME_KALMAN: "",
                                   HPC_SERVICE_NAME_SAVGOL: "",
                                   HPC_SERVICE_NAME_BLACK_SCHOLES: "",
                                   HPC_SERVICE_NAME_KMEANS: "",
                                   HPC_SERVICE_NAME_KNN: "",
                                   HPC_SERVICE_NAME_WAVELET: ""}

        self.__data_to_hpc_path = {HPC_SERVICE_NAME_FFT: "",
                                   HPC_SERVICE_NAME_KALMAN: "",
                                   HPC_SERVICE_NAME_SAVGOL: "",
                                   HPC_SERVICE_NAME_BLACK_SCHOLES: "",
                                   HPC_SERVICE_NAME_KMEANS: "",
                                   HPC_SERVICE_NAME_KNN: "",
                                   HPC_SERVICE_NAME_WAVELET: ""}

        self.__description = {"services": [self.__kernel_name], "infrastructure": "excess_slurm"}

        if self.__kernel_name == HPC_SERVICE_NAME_FFT:
            self.__description["params"] = {"read_input_data": "/Init_Data/raw_data_input/from_s3_%s" % self.__arguments[0]}
        elif self.__kernel_name == HPC_SERVICE_NAME_SAVGOL:
            self.__description["params"] = {"read_input_data": "/Init_Data/raw_data_input/from_s3_%s" % self.__arguments[0]}
        elif self.__kernel_name == HPC_SERVICE_NAME_KALMAN:
            self.__description["params"] = {"read_input_data": "/Init_Data/raw_data_input/from_s3_%s" % self.__arguments[0]}
        elif self.__kernel_name == HPC_SERVICE_NAME_BLACK_SCHOLES:
            self.__description["params"] = { "read_input_data": "/Init_Data/raw_data_input/from_s3_%s" % self.__arguments[0]}
        elif self.__kernel_name == HPC_SERVICE_NAME_KMEANS:
            self.__description["params"] = {"read_input_data": "/Init_Data/raw_data_position/from_s3_%s" % self.__arguments[0]}
        elif self.__kernel_name == HPC_SERVICE_NAME_KNN:
            self.__description["params"] = {"read_input_data": "/Init_Data/raw_data_position/from_s3_%s" % self.__arguments[0],
                                            "inference_knn_path": "/Init_Data/inference_data_position/"}

    def to_dict(self):
        return self.__description

    def get_kernel_results_filename(self):
        return self.__results_filename[self.__kernel_name]

    def get_data_to_hpc_dst_path(self):
        return self.__data_to_hpc_path[self.__kernel_name]
