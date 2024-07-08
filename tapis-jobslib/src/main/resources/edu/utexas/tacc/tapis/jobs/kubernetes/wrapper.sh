

function usage() {
  echo "Usage: $(basename $0) [launch|status|events|logs|cleanup]"
}

function get_pod_names() {
  pod_names=$(kubectl get pods --selector=job-name=$1 --output=jsonpath='{.items[*].metadata.name}')
}

function get_events() {
  get_pod_names $1

  kubectl get events --field-selector=involvedObject.name=$1

  for name in $pod_names; do
    kubectl get events --field-selector=involvedObject.name=$name | tail -n +2
  done
}


if [[ $# -eq 0 ]]; then
  action="launch"
elif [[ $# -eq 1 ]]; then
  action=$1
else
  usage

  exit 1
fi

case "$action" in
  launch)
    kubectl apply -f ${MANIFEST} > /dev/null

    active=$(kubectl get job ${JOBID} --output=jsonpath='{.status.active}')

    if [[ -z $active ]]; then
      get_events ${JOBID}

      exit 1
    fi

    echo ${JOBID}
    ;;
  status)
    get_pod_names ${JOBID}

    condition=$(kubectl get job ${JOBID} --output=jsonpath='{.status.conditions[?(@.status=="True")].type}')

    if [[ -n $condition ]]; then
      result="{\"status\":\"$condition\""

      if [[ $condition == "Complete" || $condition == "Failed" ]]; then
        for pod in $pod_names; do
          container_names=$(kubectl get pod $pod -o jsonpath='{.status.containerStatuses[*].name}')

          container_status=""

          for container in $container_names; do
            code=$(kubectl get pod $pod -o jsonpath="{.status.containerStatuses[?(@.name==\"$container\")].state.terminated.exitCode}")

            container_status="$container_status${container_status:+,}{\"name\":\"$container\",\"exitCode\":$code}"
          done

          pod_status="$pod_status${pod_status:+,}{\"name\":\"$pod\",\"containers\":[$container_status]}"
        done

        result="$result,\"pods\":[$pod_status]"
      fi

      echo "$result}"
    else
      for pod in $pod_names; do
        phase=$(kubectl get pod $pod --output=jsonpath='{.status.phase}')

        if [[ $phase != "Pending" ]]; then
          phase="Running"

          break;
        fi
      done

      echo "{\"status\":\"$phase\"}"
    fi
    ;;
  events)
    get_events ${JOBID}
    ;;
  logs)
    get_pod_names ${JOBID}

    for name in $pod_names; do
      logs=$(kubectl logs $name --all-containers=true)

      echo -e "$name:\n$logs"
    done
    ;;
  cleanup)
    kubectl delete job ${JOBID}
    ;;
  *)
    usage

    exit 1
esac
