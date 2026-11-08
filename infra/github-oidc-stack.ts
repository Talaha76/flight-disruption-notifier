import { CfnOutput, Duration, Stack, type StackProps } from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import type { Construct } from 'constructs';

export interface GithubOidcStackProps extends StackProps {
  /** "owner/repo" */
  githubRepo: string;
  /** ARN of an existing GitHub OIDC provider in the account (only one is allowed per account). */
  existingProviderArn?: string;
}

/**
 * One-time stack: lets GitHub Actions (main branch + PRs of one repo) assume a role via OIDC.
 * The role can only assume the CDK bootstrap roles, so deploy permissions stay with CDK's
 * least-privilege bootstrap roles and no long-lived AWS keys are stored in GitHub.
 */
export class GithubOidcStack extends Stack {
  constructor(scope: Construct, id: string, props: GithubOidcStackProps) {
    super(scope, id, props);
    const provider = props.existingProviderArn
      ? iam.OpenIdConnectProvider.fromOpenIdConnectProviderArn(this, 'Provider', props.existingProviderArn)
      : new iam.OpenIdConnectProvider(this, 'Provider', { url: 'https://token.actions.githubusercontent.com', clientIds: ['sts.amazonaws.com'] });

    const role = new iam.Role(this, 'DeployRole', {
      roleName: 'fdn-github-deploy',
      maxSessionDuration: Duration.hours(1),
      assumedBy: new iam.WebIdentityPrincipal(provider.openIdConnectProviderArn, {
        StringEquals: { 'token.actions.githubusercontent.com:aud': 'sts.amazonaws.com' },
        StringLike: { 'token.actions.githubusercontent.com:sub': [`repo:${props.githubRepo}:ref:refs/heads/main`, `repo:${props.githubRepo}:pull_request`] },
      }),
    });
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['sts:AssumeRole'],
      resources: [`arn:aws:iam::${this.account}:role/cdk-*`],
    }));
    new CfnOutput(this, 'DeployRoleArn', { value: role.roleArn });
  }
}
