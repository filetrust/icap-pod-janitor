package cleaner

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	listSize  = 10
	ok        = "ok"
	procErr   = "proc_error"
	delErr    = "delete_error"
	k8sconfig = "k8s_config_error"
)

var (
	procTime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "gw_pod_janitor_processing_time_millisecond",
			Help:    "Time taken for pod janitor to process",
			Buckets: []float64{5, 10, 100, 250, 500, 1000},
		},
	)

	procPodTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gw_pod_janitor_cleanup_total",
			Help: "Number of pods cleaned up by pod janitor",
		},
		[]string{"status"},
	)
)

type CleanerArgs struct {
	PodNamespace          string
	Client                *kubernetes.Clientset
	DeleteSuccessfulAfter time.Duration
	DeleteFailedAfter     time.Duration
}

func NewCleanerArgs(podNamespace string, deleteSuccessfulAfter, deleteFailedAfter time.Duration) (*CleanerArgs, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	cleanerArgs := &CleanerArgs{
		PodNamespace:          podNamespace,
		Client:                client,
		DeleteSuccessfulAfter: deleteSuccessfulAfter,
		DeleteFailedAfter:     deleteFailedAfter,
	}

	return cleanerArgs, nil
}

func PushMetrics(pushgatewayEndpoint string) {
	if err := push.New(pushgatewayEndpoint, "pod-janitor").
		Collector(procTime).
		Collector(procPodTotal).
		Format(expfmt.FmtText).
		Push(); err != nil {
		log.Printf("Failed to push metrics: %v", err)
	}
}

func (ca CleanerArgs) RunCleaner() {
	defer func(start time.Time) {
		procTime.Observe(float64(time.Since(start).Milliseconds()))
	}(time.Now())

	err := ca.processPodList("status.phase=Succeeded")
	if err != nil {
		log.Printf("Failed to process succeeded Pods: %v", err)
	}
	err = ca.processPodList("status.phase=Failed")
	if err != nil {
		log.Printf("Failed to process failed Pods: %v", err)
	}
}

func (ca CleanerArgs) processPodList(selector string) error {
	pods, err := ca.Client.CoreV1().Pods(ca.PodNamespace).List(context.TODO(), metav1.ListOptions{
		FieldSelector: selector,
		Limit:         10,
	})
	if err != nil {
		procPodTotal.WithLabelValues(procErr).Inc()
		return fmt.Errorf("Failed to get list of pods for %v: %v", selector, err)
	}
	var cont string
	cont = pods.Continue
	ca.clean(&pods.Items)
	for cont != "" {
		pods, err := ca.Client.CoreV1().Pods(ca.PodNamespace).List(context.TODO(), metav1.ListOptions{
			FieldSelector: selector,
			Limit:         listSize,
			Continue:      cont,
		})
		if err != nil {
			procPodTotal.WithLabelValues(procErr).Inc()
			return fmt.Errorf("Failed to get list of pods for %v: %v", selector, err)
		}
		cont = pods.Continue
		ca.clean(&pods.Items)
	}
	return nil
}

func (ca CleanerArgs) clean(pods *[]corev1.Pod) {
	for _, pod := range *pods {

		if shouldDeletePod(&pod, ca.DeleteSuccessfulAfter, ca.DeleteFailedAfter) {
			err := ca.Client.CoreV1().Pods(ca.PodNamespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})

			if err != nil {
				procPodTotal.WithLabelValues(delErr).Inc()
				log.Printf("Failed to delete pod: %s %v", pod.Name, err)
				continue
			}

			procPodTotal.WithLabelValues(ok).Inc()
			log.Printf("Cleaned up pod %s", pod.Name)
		}
	}
}

func shouldDeletePod(pod *corev1.Pod, successful, failed time.Duration) bool {
	podFinishTime := podFinishTime(pod)

	if !podFinishTime.IsZero() {
		age := time.Since(podFinishTime)

		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			if successful > 0 && age >= successful {
				return true
			}
		case corev1.PodFailed:
			if failed > 0 && age >= failed {
				return true
			}
		default:
			return false
		}
	}

	return false
}

func podFinishTime(podObj *corev1.Pod) time.Time {
	for _, pc := range podObj.Status.Conditions {
		// Looking for the time when pod's condition "Ready" became "false" (equals end of execution)
		if pc.Type == corev1.PodReady && pc.Status == corev1.ConditionFalse {
			return pc.LastTransitionTime.Time
		}
	}

	return time.Time{}
}
