import { describe, expect, it } from 'vitest';
import { diffSuitePayloads, diffPolicyPayloads } from './revisionDiffUtils';
import type { Suite, Policy } from '../../api/types';

describe('revisionDiffUtils', () => {
  describe('diffSuitePayloads', () => {
    it('detects added, removed, changed rules by rule_id regardless of array order', () => {
      const left: Suite = {
        format: 'dq-rules/v3',
        description: 'v1',
        rules: [
          {
            rule_id: 'r1',
            dimension: 'validity',
            scope: 'PARTITION',
            stage: 'CANDIDATE',
            check: { kind: 'complete', column: 'user_id', assert: { op: '==', value: '1' } },
          },
          {
            rule_id: 'r2',
            dimension: 'accuracy',
            scope: 'PARTITION',
            stage: 'CANDIDATE',
            check: { kind: 'range', column: 'age', min: '0', max: '120', assert: { op: '==', value: '1' } },
          },
        ],
      };

      const right: Suite = {
        format: 'dq-rules/v3',
        description: 'v2',
        rules: [
          {
            rule_id: 'r2', // reordered to index 0
            dimension: 'accuracy',
            scope: 'PARTITION',
            stage: 'CANDIDATE',
            check: { kind: 'range', column: 'age', min: '18', max: '120', assert: { op: '==', value: '1' } }, // min changed
          },
          {
            rule_id: 'r3', // added
            dimension: 'completeness',
            scope: 'PARTITION',
            stage: 'CANDIDATE',
            check: { kind: 'complete', column: 'email', assert: { op: '==', value: '1' } },
          },
        ],
      };

      const diff = diffSuitePayloads(left, right);
      expect(diff.suiteLevelChanges).toEqual([
        { field: 'description', left: 'v1', right: 'v2' },
      ]);

      const r1 = diff.ruleChanges.find(r => r.rule_id === 'r1');
      expect(r1?.changeType).toBe('removed');

      const r3 = diff.ruleChanges.find(r => r.rule_id === 'r3');
      expect(r3?.changeType).toBe('added');

      const r2 = diff.ruleChanges.find(r => r.rule_id === 'r2');
      expect(r2?.changeType).toBe('changed');
      if (r2 && r2.changeType === 'changed') {
        expect(r2.changes.some(c => c.path === 'check.min' && c.left === '0' && c.right === '18')).toBe(true);
      }
    });
  });

  describe('diffPolicyPayloads', () => {
    it('detects changes in rule actions, required, and global anomaly/ratio thresholds', () => {
      const left: Policy = {
        rules: [
          { rule_id: 'r1', action: 'OBSERVE', required: true },
        ],
        max_quarantine_ratio: '0.05',
        anomaly: { enabled: false },
      };

      const right: Policy = {
        rules: [
          { rule_id: 'r1', action: 'QUARANTINE', required: true },
        ],
        max_quarantine_ratio: '0.1',
        anomaly: { enabled: true, max_points: 30, lower: '0', upper: '1', bootstrap: false },
      };

      const diff = diffPolicyPayloads(left, right);
      expect(diff.globalChanges.some(g => g.field === 'max_quarantine_ratio' && g.left === '0.05' && g.right === '0.1')).toBe(true);
      expect(diff.globalChanges.some(g => g.field === 'anomaly.enabled' && g.left === false && g.right === true)).toBe(true);

      const r1 = diff.ruleChanges.find(r => r.rule_id === 'r1');
      expect(r1?.changeType).toBe('changed');
      if (r1 && r1.changeType === 'changed') {
        expect(r1.changes.some(c => c.path === 'action' && c.left === 'OBSERVE' && c.right === 'QUARANTINE')).toBe(true);
      }
    });
  });
});
