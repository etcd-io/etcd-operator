# Roadmap

The list is based on the survey results and the discussion in wg-etcd-operator. It is not set in stone and may be adjusted as needed.

## v0.1.0
- Create a new etcd cluster, e.g 3 or 5 members cluster
  - Users should be able to set at least the etcd version and cluster size
- Understand health of a cluster
- Enabling TLS communication
  - Should also support certificate renewal

## v0.2.0
- Upgrade across patches or one minor version
- Scale in and out, e.g 1 -> 3 -> 5 members and vice versa
- Support customizing etcd options (via flags or env vars)

## v0.3.0
- Recover a single failed cluster member (still have quorum)
- Recover from multiple failed cluster members (quorum loss)

## v0.4.0
- Create on-demand backup of a cluster
- Create periodic backup of a cluster
- Create a new cluster from a backup

## Future versions
It makes no sense to plan too far ahead because plans can't keep up with changes.
