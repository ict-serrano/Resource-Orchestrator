# Resource-Orchestrator
SERRANO - Resource Orchestrator

## Description

Resource Orchestrator is cloud-navite application written in Python that faciliates the seamless orchestation of cloud-native applications across federated edge,cloud and HPC platforms. It was impemented in the context of [SERRANO](https://ict-serrano.eu) Horizon 2020 project.

The SERRANO Resource Orchestrator acts as a high-level orchestrator that interacts with multiple Local Orchestrators, where each handles individual parts of the overall unified infrastructure. 

It uses the scheduling capabilities of the Resource Orchestration Toolkit (ROT) that provides cognitive decisions. Then, it delegates, through the implemented Orchestration Drivers, the decision for the actual deployment operations to the corresponding Local Orchestrators at the selected platforms. 


## Requirements
The required dependencies are found in the `requirements.txt` file. To install them, run `pip install -r requirements.txt` from the project folder.


## Additional information

More details are available in SERRANO Deliverables D5.3 (M15) and D5.4 (M31) in the [SERRANO project](https://ict-serrano.eu/deliverables/) web site.


