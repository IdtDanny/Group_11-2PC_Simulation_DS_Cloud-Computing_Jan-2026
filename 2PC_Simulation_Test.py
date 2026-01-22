import random
import time


class Participant:
    def __init__(self, name, rate_failure=0.1):
        self.name = name
        self.rate_failure = rate_failure
        self.prepared = False
        self.committed = False

    def prepare(self):
        """Phase 1: Prepare to commit"""
        print(f"  [{self.name}] Preparing Transaction...")
        time.sleep(0.3)

        # Simulate random failures
        if random.random() < self.rate_failure:
            print(f"  [{self.name}] ❌ VOTE NO - Cannot prepare")
            return False

        self.prepared = True
        print(f"  [{self.name}] ✓ VOTE YES - Ready to commit")
        return True

    def commit(self):
        """Phase 2: Commit the transaction"""
        if not self.prepared:
            print(f"  [{self.name}] ❌ Cannot commit - not prepared")
            return False

        print(f"  [{self.name}] Committing transaction...")
        time.sleep(0.2)
        self.committed = True
        print(f"  [{self.name}] ✓ Transaction committed")
        return True

    def abort(self):
        """Abort the transaction"""
        print(f"  [{self.name}] Rolling back transaction...")
        time.sleep(0.2)
        self.prepared = False
        self.committed = False
        print(f"  [{self.name}] ✓ Transaction aborted")


class Coordinator:
    def __init__(self, participants):
        self.participants = participants

    def execute_transaction(self, transaction_id):
        """Execute 2PC protocol"""
        print(f"\n{'=' * 60}")
        print(f"Starting Transaction {transaction_id}")
        print(f"{'=' * 60}")

        # PHASE 1: PREPARE
        print(f"\n--- PHASE 1: PREPARE ---")
        print("[Coordinator] Sending PREPARE request to all participants...")

        votes = []
        for participant in self.participants:
            vote = participant.prepare()
            votes.append(vote)

        # Check if all voted YES
        all_yes = all(votes)

        # PHASE 2: COMMIT or ABORT
        print(f"\n--- PHASE 2: DECISION ---")

        if all_yes:
            print("[Coordinator] All participants voted YES")
            print("[Coordinator] Decision: COMMIT")
            print("[Coordinator] Sending COMMIT to all participants...\n")

            for participant in self.participants:
                participant.commit()

            print(f"\n{'=' * 60}")
            print(f"✓ Transaction {transaction_id} COMMITTED successfully")
            print(f"{'=' * 60}")
            return True
        else:
            print("[Coordinator] At least one participant voted NO")
            print("[Coordinator] Decision: ABORT")
            print("[Coordinator] Sending ABORT to all participants...\n")

            for participant in self.participants:
                participant.abort()

            print(f"\n{'=' * 60}")
            print(f"❌ Transaction {transaction_id} ABORTED")
            print(f"{'=' * 60}")
            return False


# Example usage
if __name__ == "__main__":
    print("Two-Phase Commit Protocol Simulation")
    print("=====================================\n")

    # Create participants (databases, services, etc.)
    participants = [
        Participant("Database-A", rate_failure=0.2),
        Participant("Database-B", rate_failure=0.1),
        Participant("Database-C", rate_failure=0.2)
    ]

    # Create coordinator
    coordinator = Coordinator(participants)

    # Run multiple transactions
    for i in range(1, 4):
        coordinator.execute_transaction(f"TXN-{i}")
        time.sleep(1)

    # Summary
    print("\n\nSUMMARY")
    print("=" * 60)
    for p in participants:
        status = "COMMITTED" if p.committed else "ABORTED"
        print(f"{p.name}: {status}")