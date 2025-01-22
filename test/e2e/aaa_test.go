package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	gofail "go.etcd.io/gofail/runtime"

	"go.etcd.io/etcd-operator/test/utils"
)

// namespace, serviceAccountName, metricsServiceName などは他ファイルで
// すでに定義されているものを再利用。
// ここでは scale_failpoint_test.go 独自のテストケースを追加するイメージです.

// EtcdClusterの名前を使い回すため、定数として定義しておきます。
const failpointEtcdClusterName = "test-failpoint"

var _ = Describe("Scale Failpoint Tests", Ordered, func() {
	var controllerPodName string

	BeforeAll(func() {
		By("Creating an EtcdCluster with size=1 as base")

		// 適宜、使い回せるようにEtcdClusterリソースをyaml文字列で用意する
		// 一例。versionやnamespaceなどは必要に応じて書き換え
		etcdClusterYAML := fmt.Sprintf(`
apiVersion: operator.etcd.io/v1alpha1
kind: EtcdCluster
metadata:
  name: %s
  namespace: %s
spec:
  size: 1
  version: 3.5.17
`, failpointEtcdClusterName, namespace)

		// Temporaryファイルに書いてkubectlでapply
		tempFile := filepath.Join(os.TempDir(), "failpoint-cluster.yaml")
		Expect(os.WriteFile(tempFile, []byte(etcdClusterYAML), 0600)).To(Succeed())

		cmd := exec.Command("kubectl", "apply", "-f", tempFile)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create base EtcdCluster resource")

		// EtcdCluster コントローラが起動していることを確認 (controllerPodName の取得など)
		verifyOperatorIsRunning := func(g Gomega) {
			// operatorのPod名を取得
			cmd = exec.Command("kubectl", "get",
				"pods", "-l", "control-plane=controller-manager",
				"-o", "go-template={{ range .items }}"+
					"{{ if not .metadata.deletionTimestamp }}"+
					"{{ .metadata.name }}"+
					"{{ \"\\n\" }}{{ end }}{{ end }}",
				"-n", namespace,
			)
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred(), "Failed to get the operator controller-manager pod")
			podNames := utils.GetNonEmptyLines(out)
			g.Expect(podNames).To(HaveLen(1))
			controllerPodName = podNames[0]

			// PodがRunningになるまで待つ
			cmd = exec.Command("kubectl", "get",
				"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
				"-n", namespace,
			)
			phase, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(phase).To(Equal("Running"), "Operator pod is not in Running phase yet")
		}
		Eventually(verifyOperatorIsRunning).Should(Succeed())

		// EtcdClusterが size=1 で安定するまで待つ (StatefulSet が 1replica になれば良い)
		verifySTS := func(g Gomega) {
			cmd = exec.Command("kubectl", "get", "statefulset", failpointEtcdClusterName,
				"-n", namespace, "-o", "jsonpath={.status.readyReplicas}")
			out, err := utils.Run(cmd)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(out).To(Equal("1"), "StatefulSet did not become 1 replica for base EtcdCluster")
		}
		Eventually(verifySTS, 2*time.Minute, 5*time.Second).Should(Succeed())
	})

	AfterAll(func() {
		By("Cleaning up the test EtcdCluster resource")
		// 念のためリソース削除
		cmd := exec.Command("kubectl", "delete", "etcdcluster", failpointEtcdClusterName,
			"-n", namespace, "--ignore-not-found=true")
		_, _ = utils.Run(cmd)
	})

	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching operator logs for debugging")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			logs, err := utils.Run(cmd)
			if err == nil {
				fmt.Fprintf(GinkgoWriter, "Operator logs:\n%s\n", logs)
			} else {
				fmt.Fprintf(GinkgoWriter, "Failed to get Operator logs: %v\n", err)
			}
		}
	})

	Context("Scale Out with CrashAfterAddMember", func() {
		It("should crash after adding new member, then eventually scale up to 2 replicas", func() {
			By("Enabling CrashAfterAddMember failpoint")
			err := gofail.Enable("CrashAfterAddMember", `return("simulate panic after AddMember")`)
			Expect(err).NotTo(HaveOccurred())

			By("Increasing size from 1 to 2 to trigger scale out")
			cmd := exec.Command("kubectl", "patch", "etcdcluster", failpointEtcdClusterName,
				"-n", namespace,
				"--type=merge", "--patch", `{"spec":{"size":2}}`)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for operator to crash (pod will go into CrashLoopBackOff or Terminating)")
			checkCrash := func(g Gomega) {
				cmd = exec.Command("kubectl", "get", "pod", controllerPodName,
					"-n", namespace, "-o", "jsonpath={.status.containerStatuses[*].state}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				// どのようにpanicを検知するかはクラスタの実装次第ですが、
				// CrashLoopBackOffなどの文字列や restarted: 1 などを見てもよい
				Expect(out).To(ContainSubstring("terminated"), "Operator did not appear to crash")
			}
			Eventually(checkCrash, 90*time.Second, 5*time.Second).Should(Succeed())

			By("Waiting for operator to come back as Running after the crash")
			verifyOperatorRestarted := func(g Gomega) {
				// Pod再起動で名前が変わる場合は get pods して取り直し
				// もし同じPodが再利用されるなら logsなどで再確認
				// 例として同じpod名でphase=Runningになるまで待つ
				cmd = exec.Command("kubectl", "get", "pod", controllerPodName,
					"-n", namespace, "-o", "jsonpath={.status.phase}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("Running"))
			}
			Eventually(verifyOperatorRestarted, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("Ensuring the STS finally is scaled up to 2 replicas")
			verifySTS2 := func(g Gomega) {
				cmd = exec.Command("kubectl", "get", "statefulset", failpointEtcdClusterName,
					"-n", namespace, "-o", "jsonpath={.status.readyReplicas}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("2"))
			}
			Eventually(verifySTS2, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("Disabling CrashAfterAddMember failpoint to avoid interfering with next tests")
			err = gofail.Disable("CrashAfterAddMember")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Scale In with CrashAfterRemoveMember", func() {
		// 前のテストで size=2 に上がっているので、ここで2 → 1に下げる
		It("should crash after removing member, then eventually scale down to 1 replica", func() {
			By("Enabling CrashAfterRemoveMember failpoint")
			err := gofail.Enable("CrashAfterRemoveMember", `return("simulate panic after RemoveMember")`)
			Expect(err).NotTo(HaveOccurred())

			By("Decreasing size from 2 to 1 to trigger scale in")
			cmd := exec.Command("kubectl", "patch", "etcdcluster", failpointEtcdClusterName,
				"-n", namespace,
				"--type=merge", "--patch", `{"spec":{"size":1}}`)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred())

			By("Waiting for operator to crash (pod will go into CrashLoopBackOff or Terminating)")
			checkCrash := func(g Gomega) {
				cmd = exec.Command("kubectl", "get", "pod", controllerPodName,
					"-n", namespace, "-o", "jsonpath={.status.containerStatuses[*].state}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				Expect(out).To(ContainSubstring("terminated"), "Operator did not appear to crash on remove")
			}
			Eventually(checkCrash, 90*time.Second, 5*time.Second).Should(Succeed())

			By("Waiting for operator to come back as Running after the crash")
			verifyOperatorRestarted := func(g Gomega) {
				cmd = exec.Command("kubectl", "get", "pod", controllerPodName,
					"-n", namespace, "-o", "jsonpath={.status.phase}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("Running"))
			}
			Eventually(verifyOperatorRestarted, 2*time.Minute, 5*time.Second).Should(Succeed())

			By("Ensuring the STS finally is scaled down to 1 replica")
			verifySTS1 := func(g Gomega) {
				cmd = exec.Command("kubectl", "get", "statefulset", failpointEtcdClusterName,
					"-n", namespace, "-o", "jsonpath={.status.readyReplicas}")
				out, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(out).To(Equal("1"))
			}
			Eventually(verifySTS1, 3*time.Minute, 5*time.Second).Should(Succeed())

			By("Disabling CrashAfterRemoveMember failpoint")
			err = gofail.Disable("CrashAfterRemoveMember")
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
