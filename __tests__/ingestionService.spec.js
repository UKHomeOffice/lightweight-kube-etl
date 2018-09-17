"use strict";

describe("ingestionService", () => {

    describe("runService", () => {

        it("", () => {

           expect(true).toEqual(true);
        });
    });

});


// it("should delete jobs with the same role", () => {
//
//   return kube
//     .runKubeJob("MockJobName", "mockTimestamp")
//     .then(() => {
//       expect(childProcess.exec.mock.calls[0][0])
//         .toEqual("/app/kubectl --token MOCK_TOKEN delete job -l role=mockRole")
//     })
// })
//
// it("should create a new job", () => {
//   return kube
//     .runKubeJob("MockJobName", "mockTimestamp")
//     .then(() => {
//       expect(childProcess.exec.mock.calls[1][0])
//         .toEqual("/app/kubectl --token MOCK_TOKEN create job MockJobName-mockTimestamp --from=cronjob/MockJobName")
//     })
// })
//
// it("should label the started job with a role", () => {
//   return kube
//     .runKubeJob("MockJobName", "mockTimestamp")
//     .then(() => {
//       expect(childProcess.exec.mock.calls[2][0])
//         .toEqual("/app/kubectl --token MOCK_TOKEN label job MockJobName-mockTimestamp role=mockRole")
//     })
// })

// describe.only("after creating a job", () => {
//
//   it("should wait for job to complete", () => {
//
//     expect(kube.waitForJob).toEqual(false);
//
//   });
//
// });


// it("should start the kube job if the manifest is good", async () => {
//   await etl.sqsMessageHandler(mockMessages[0], doneMock)
//
//   expect(doneMock).toHaveBeenCalledTimes(1)
//   expect(ingestionService.runIngest).toHaveBeenCalledTimes(1)
//   expect(ingestionService.runIngest.mock.calls[0][0]).toBe('delta')
//   expect(ingestionService.runIngest.mock.calls[0][1]).toBe('222222222333')
//   // expect(ingestionService.runIngest.mock.calls[1][0]).toBe('elastic-delta')
//   // expect(ingestionService.runIngest.mock.calls[1][1]).toBe('222222222333')
// })