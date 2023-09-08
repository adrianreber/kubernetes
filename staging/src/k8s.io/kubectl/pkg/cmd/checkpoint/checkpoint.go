/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package checkpoint

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/resource"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"

	"k8s.io/apimachinery/pkg/api/meta"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/util/podcmd"
	"k8s.io/kubectl/pkg/polymorphichelpers"
	"k8s.io/kubectl/pkg/scheme"
	"k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/interrupt"
	"k8s.io/kubectl/pkg/util/templates"
	"k8s.io/kubectl/pkg/util/term"
)

var (
	checkpointExample = templates.Examples(i18n.T(`
		# Get output from running the 'date' command from pod mypod, using the first container by default
		kubectl exec mypod -- date

		# Get output from running the 'date' command in ruby-container from pod mypod
		kubectl exec mypod -c ruby-container -- date

		# Switch to raw terminal mode; sends stdin to 'bash' in ruby-container from pod mypod
		# and sends stdout/stderr from 'bash' back to the client
		kubectl exec mypod -c ruby-container -i -t -- bash -il

		# List contents of /usr from the first container of pod mypod and sort by modification time
		# If the command you want to execute in the pod has any flags in common (e.g. -i),
		# you must use two dashes (--) to separate your command's flags/arguments
		# Also note, do not surround your command and its flags/arguments with quotes
		# unless that is how you would execute it normally (i.e., do ls -t /usr, not "ls -t /usr")
		kubectl exec mypod -i -t -- ls -t /usr

		# Get output from running 'date' command from the first pod of the deployment mydeployment, using the first container by default
		kubectl exec deploy/mydeployment -- date

		# Get output from running 'date' command from the first pod of the service myservice, using the first container by default
		kubectl exec svc/myservice -- date
		`))
)

const (
	defaultPodCheckpointTimeout = 60 * time.Second
)

func NewCmdCheckpoint(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	options := &ExecOptions{}
	cmd := &cobra.Command{
		Use:                   "checkpoint (POD | TYPE/NAME) [-c CONTAINER] [flags]",
		DisableFlagsInUseLine: true,
		Short:                 i18n.T("Checkpoint a container"),
		Long:                  i18n.T("Checkpoint a container."),
		Example:               checkpointExample,
		ValidArgsFunction:     completion.PodResourceNameCompletionFunc(f),
		Run: func(cmd *cobra.Command, args []string) {
			argsLenAtDash := cmd.ArgsLenAtDash()
			cmdutil.CheckErr(options.Complete(f, cmd, args, argsLenAtDash))
			cmdutil.CheckErr(options.Validate())
			cmdutil.CheckErr(options.Run(f))
		},
	}
	cmdutil.AddPodRunningTimeoutFlag(cmd, defaultPodCheckpointTimeout)
	cmdutil.AddContainerVarFlags(cmd, &options.ContainerName, options.ContainerName)
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc("container", completion.ContainerCompletionFunc(f)))

	return cmd
}

type StreamOptions struct {
	Namespace     string
	PodName       string
	ContainerName string
	Stdin         bool
	TTY           bool
	// minimize unnecessary output
	Quiet bool
	// InterruptParent, if set, is used to handle interrupts while attached
	InterruptParent *interrupt.Handler

	genericiooptions.IOStreams

	// for testing
	overrideStreams func() (io.ReadCloser, io.Writer, io.Writer)
	isTerminalIn    func(t term.TTY) bool
}

// ExecOptions declare the arguments accepted by the Exec command
type ExecOptions struct {
	StreamOptions
	resource.FilenameOptions

	ResourceName     string
	Command          []string
	EnforceNamespace bool

	Builder          func() *resource.Builder
	ExecutablePodFn  polymorphichelpers.AttachablePodForObjectFunc
	restClientGetter genericclioptions.RESTClientGetter

	Pod *corev1.Pod
	//Executor      RemoteExecutor
	PodClient     coreclient.PodsGetter
	GetPodTimeout time.Duration
	Config        *restclient.Config
}

// Complete verifies command line arguments and loads data from the command environment
func (p *ExecOptions) Complete(f cmdutil.Factory, cmd *cobra.Command, argsIn []string, argsLenAtDash int) error {
	if len(argsIn) > 0 && argsLenAtDash != 0 {
		p.ResourceName = argsIn[0]
	}
	if argsLenAtDash > -1 {
		p.Command = argsIn[argsLenAtDash:]
	} else if len(argsIn) > 1 {
		if !p.Quiet {
			fmt.Fprint(p.ErrOut, "kubectl exec [POD] [COMMAND] is DEPRECATED and will be removed in a future version. Use kubectl exec [POD] -- [COMMAND] instead.\n")
		}
		p.Command = argsIn[1:]
	} else if len(argsIn) > 0 && len(p.FilenameOptions.Filenames) != 0 {
		if !p.Quiet {
			fmt.Fprint(p.ErrOut, "kubectl exec [POD] [COMMAND] is DEPRECATED and will be removed in a future version. Use kubectl exec [POD] -- [COMMAND] instead.\n")
		}
		p.Command = argsIn[0:]
		p.ResourceName = ""
	}

	var err error
	p.Namespace, p.EnforceNamespace, err = f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return err
	}

	p.ExecutablePodFn = polymorphichelpers.AttachablePodForObjectFn

	p.GetPodTimeout, err = cmdutil.GetPodRunningTimeoutFlag(cmd)
	if err != nil {
		return cmdutil.UsageErrorf(cmd, err.Error())
	}

	p.Builder = f.NewBuilder
	p.restClientGetter = f

	p.Config, err = f.ToRESTConfig()
	if err != nil {
		return err
	}

	clientset, err := f.KubernetesClientSet()
	if err != nil {
		return err
	}
	p.PodClient = clientset.CoreV1()

	return nil
}

func (p *ExecOptions) Validate() error {
	if len(p.PodName) == 0 && len(p.ResourceName) == 0 {
		return fmt.Errorf("pod or type/name must be specified")
	}
	return nil
}

// Run tries to checkpoint the given container in the given pod.
func (p *ExecOptions) Run(f cmdutil.Factory) error {
	builder := p.Builder().
		WithScheme(scheme.Scheme, scheme.Scheme.PrioritizedVersionsAllGroups()...).
		FilenameParam(p.EnforceNamespace, &p.FilenameOptions).
		NamespaceParam(p.Namespace).DefaultNamespace()
	if len(p.ResourceName) > 0 {
		builder = builder.ResourceNames("pods", p.ResourceName)
	}

	obj, err := builder.Do().Object()
	if err != nil {
		return err
	}

	if meta.IsListType(obj) {
		return fmt.Errorf("cannot checkpoint multiple objects at a time")
	}

	p.Pod, err = p.ExecutablePodFn(p.restClientGetter, obj, p.GetPodTimeout)
	if err != nil {
		return err
	}

	pod := p.Pod

	if pod.Status.Phase != corev1.PodRunning {
		return fmt.Errorf("cannot checkpoint a container in non-running pod; current phase is %s", pod.Status.Phase)
	}

	containerName := p.ContainerName
	if len(containerName) == 0 {
		container, err := podcmd.FindOrDefaultContainerByName(pod, containerName, p.Quiet, p.ErrOut)
		if err != nil {
			return err
		}
		containerName = container.Name
	}

	restClient, err := restclient.RESTClientFor(p.Config)
	if err != nil {
		return err
	}
	req := restClient.Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("checkpoint")
	req.VersionedParams(&corev1.PodCheckpointOptions{
		Container: containerName,
	}, scheme.ParameterCodec)

	result := req.Do(context.Background())

	err = result.Error()

	if err != nil {
		return err
	}

	var statusCode int
	result = result.StatusCode(&statusCode)

	if statusCode != http.StatusOK {
		return fmt.Errorf(
			"unexpected status code (%d) returned. Expected %d",
			statusCode,
			http.StatusOK,
		)
	}

	body, err := result.Raw()
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", string(body))

	return nil
}
